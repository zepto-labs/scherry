package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/zepto-labs/scherry"
)

const (
	payrollJobName = "monthly_payroll_job"
	bonusJobName   = "bonus_payout_job"
)

func main() {
	logger := scherry.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	redisAddr := envOr("REDIS_ADDR", "127.0.0.1:6379")
	redisCfg := scherry.RedisConfig{Addr: redisAddr}

	svc, err := scherry.New(scherry.Config{
		ServiceName: "payroll-example",
		Logger:      logger,
		Redis:       redisCfg,
		Database: scherry.DatabaseConfig{
			Host: envOr("PG_HOST", "127.0.0.1"), Port: 5432,
			User: envOr("PG_USER", "postgres"), Password: envOr("PG_PASSWORD", "postgres"),
			DBName: envOr("PG_DB", "jobscheduler"), SSLMode: "disable",
		},
	})
	if err != nil {
		logger.Error("init scheduler failed", "error", err)
		os.Exit(1)
	}

	store := newEmployeeStore()

	// Monthly payroll — triggered automatically by the cron scheduler.
	if err := svc.Register(scherry.JobConfig{
		Name:              payrollJobName,
		JobExecutor:       &payrollBatchJobExecutor{store: store, batchSize: 2, logger: logger},
		TaskExecutor:      &payrollPayoutTaskExecutor{store: store, logger: logger},
		Schedule:          "* * * * *",
		JobCompletionCron: "* * * * *",
		AsynqOptions: []asynq.Option{
			asynq.MaxRetry(3),
			asynq.Retention(24 * time.Hour),
		},
		Retry: scherry.RetryConfig{MaxRetries: 3, RetryDelay: 2 * time.Minute},
		Hooks: scherry.Hooks{
			OnTasksPublished: func(_ context.Context, jobName string, count int) {
				logger.Info("tasks published", "job", jobName, "count", count)
			},
			OnTaskFinished: func(_ context.Context, jobName, taskID, status string, d time.Duration) {
				logger.Info("task finished", "job", jobName, "task_id", taskID, "status", status, "duration", d)
			},
		},
		Kafka: scherry.KafkaConfig{
			Brokers:        []string{envOr("KAFKA_BROKERS", "127.0.0.1:9092")},
			TaskTopic:      "payroll_payout_tasks",
			TaskRetryTopic: "payroll_payout_tasks_retry",
			TaskDLQTopic:   "payroll_payout_tasks_dlq",
			ConsumerGroup:  "payroll_payout_worker",
		},
	}); err != nil {
		logger.Error("register payroll job failed", "error", err)
		os.Exit(1)
	}

	// Bonus payout — no schedule; fired on demand via POST /bonus/trigger.
	// The bonus percentage is passed through the request body and forwarded
	// as job metadata so both the job and task executors can read it.
	if err := svc.RegisterManual(scherry.JobConfig{
		Name:              bonusJobName,
		JobExecutor:       &bonusBatchJobExecutor{store: store, batchSize: 2, logger: logger},
		TaskExecutor:      &bonusPayoutTaskExecutor{store: store, logger: logger},
		Retry:             scherry.RetryConfig{MaxRetries: 3, RetryDelay: 2 * time.Minute},
		JobCompletionCron: "* * * * *",
		Hooks: scherry.Hooks{
			OnTasksPublished: func(_ context.Context, jobName string, count int) {
				logger.Info("tasks published", "job", jobName, "count", count)
			},
			OnTaskFinished: func(_ context.Context, jobName, taskID, status string, d time.Duration) {
				logger.Info("task finished", "job", jobName, "task_id", taskID, "status", status, "duration", d)
			},
			OnJobFinished: func(_ context.Context, jobName, refID, status string, d time.Duration) {
				logger.Info("job finished", "job", jobName, "ref_id", refID, "status", status, "duration", d)
			},
		},
		Kafka: scherry.KafkaConfig{
			Brokers:        []string{envOr("KAFKA_BROKERS", "127.0.0.1:9092")},
			TaskTopic:      "bonus_payout_tasks",
			TaskRetryTopic: "bonus_payout_tasks_retry",
			TaskDLQTopic:   "bonus_payout_tasks_dlq",
			ConsumerGroup:  "bonus_payout_worker",
			// ConsumerConfig is passed straight to kafka-go; set only the
			// fields you want to tune and leave the rest at kafka-go defaults.
			ConsumerConfig: kafkago.ReaderConfig{
				MinBytes:       1_000,     // wait for at least 1 KB per fetch
				MaxBytes:       5_000_000, // cap fetch response at 5 MB
				MaxWait:        500 * time.Millisecond,
				SessionTimeout: 30 * time.Second,
				CommitInterval: time.Second, // auto-commit offsets every second
			},
			// WriterConfig is used as-is by the producer; Addr is set from
			// Brokers automatically.
			WriterConfig: &kafkago.Writer{
				Balancer:     &kafkago.Hash{},
				RequiredAcks: kafkago.RequireOne, // leader ack only — lower latency
				BatchTimeout: 5 * time.Millisecond,
				Compression:  kafkago.Snappy,
			},
		},
	}); err != nil {
		logger.Error("register bonus job failed", "error", err)
		os.Exit(1)
	}

	mux := asynq.NewServeMux()
	if err := svc.Start(context.Background(), mux); err != nil {
		logger.Error("start scheduler failed", "error", err)
		os.Exit(1)
	}

	server := asynq.NewServer(redisCfg.AsynqClientOpt(), asynq.Config{Concurrency: 5})
	go func() {
		if err := server.Run(mux); err != nil {
			logger.Error("asynq server stopped", "error", err)
		}
	}()

	// Bonus trigger API — exposes POST /bonus/trigger so callers can fire
	// an on-demand bonus payout by supplying a bonus percentage.
	apiAddr := envOr("API_ADDR", ":8080")
	apiServer := &http.Server{
		Addr:    apiAddr,
		Handler: bonusAPIHandler(svc, store, logger),
	}
	go func() {
		logger.Info("bonus API listening", "addr", apiAddr)
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("bonus API stopped", "error", err)
		}
	}()

	// History console — browse persisted job/task execution history, per-job
	// progress and timings, and trigger retries. Open http://localhost:3002
	// (override with CONSOLE_ADDR).
	consoleServer, err := svc.StartConsole(envOr("CONSOLE_ADDR", ":3002"))
	if err != nil {
		logger.Error("start console failed", "error", err)
		os.Exit(1)
	}

	logger.Info("payroll example running")
	waitForShutdown()
	svc.Stop()
	server.Shutdown()
	_ = apiServer.Shutdown(context.Background())
	_ = consoleServer.Shutdown(context.Background())
}

// bonusAPIHandler wires the bonus-related HTTP routes:
//
//	POST /bonus/trigger — fire an on-demand bonus payout run
//	GET  /bonus/payouts — list all recorded payout entries (for verification)
func bonusAPIHandler(svc *scherry.Service, store *employeeStore, logger scherry.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /bonus/trigger", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BonusPct float64 `json:"bonus_pct"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
			return
		}
		// if req.BonusPct <= 0 {
		// 	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bonus_pct must be greater than 0"})
		// 	return
		// }

		refID := uuid.New().String()
		metadata := map[string]interface{}{"bonus_pct": req.BonusPct}

		if err := svc.Trigger(r.Context(), bonusJobName, refID, metadata); err != nil {
			logger.Error("bonus trigger failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		logger.Info("bonus job triggered", "ref_id", refID, "bonus_pct", req.BonusPct)
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"ref_id":    refID,
			"job":       bonusJobName,
			"bonus_pct": req.BonusPct,
		})
	})

	mux.HandleFunc("GET /bonus/payouts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{"payouts": store.Payouts()})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func waitForShutdown() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
