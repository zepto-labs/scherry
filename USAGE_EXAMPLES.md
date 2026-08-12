# scherry Usage Examples

This guide shows how to use each **scherry** feature with concrete use cases. For architecture, API reference, and setup prerequisites, see [README.md](README.md). For a runnable end-to-end demo, see [`examples/payroll/`](examples/payroll/).

---

## Table of contents

- [Bootstrap](#bootstrap)
- [Cron-scheduled jobs](#cron-scheduled-jobs)
- [Manual / on-demand jobs](#manual--on-demand-jobs)
- [Job → task splitting](#job--task-splitting)
- [Task execution](#task-execution)
- [Task distribution keys](#task-distribution-keys)
- [Best-effort deduplication (refID)](#best-effort-deduplication-refid)
- [CancelPrevious](#cancelprevious)
- [Enabled callback](#enabled-callback)
- [Retry policy and DLQ](#retry-policy-and-dlq)
- [Programmatic job retry](#programmatic-job-retry)
- [Job completion checks](#job-completion-checks)
- [Shared Kafka consumer](#shared-kafka-consumer)
- [Kafka tuning](#kafka-tuning)
- [TLS and SASL](#tls-and-sasl)
- [History console](#history-console)
- [Execution hooks](#execution-hooks)
- [Programmatic task queries](#programmatic-task-queries)
- [Custom transport](#custom-transport)
- [Logging](#logging)
- [Async job retry via Asynq](#async-job-retry-via-asynq)

---

## Bootstrap

**Use case:** Wire scherry into your service at startup with Postgres (durable history), Redis (Asynq cron triggers), and Kafka (task fan-out).

Apply migrations from [`migrations/`](migrations/) before starting.

```go
import (
    "log/slog"
    "github.com/zepto-labs/scherry"
)

logger := scherry.NewSlogLogger(slog.Default())

svc, err := scherry.New(scherry.Config{
    ServiceName: "billing-service",
    Logger:      logger, // required
    Redis:       scherry.RedisConfig{Addr: "localhost:6379"},
    Database: scherry.DatabaseConfig{
        Host: "localhost", Port: 5432,
        User: "postgres", Password: "secret",
        DBName: "jobs", SSLMode: "disable",
    },
    Hooks: myHooks, // optional — see [Execution hooks](#execution-hooks)
})
if err != nil {
    log.Fatal(err)
}
```

Start the Asynq worker after registering jobs:

```go
mux := asynq.NewServeMux()
if err := svc.Start(ctx, mux); err != nil {
    log.Fatal(err)
}

server := asynq.NewServer(svc.AsynqRedisOpt(), asynq.Config{Concurrency: 10})
go server.Run(mux)
```

---

## Cron-scheduled jobs

**Use case:** Run a recurring batch job — nightly invoice generation, hourly cache warming, or weekly report exports.

```go
err := svc.Register(scherry.JobConfig{
    Name:              "nightly_invoice_export",
    JobExecutor:       &invoiceExportJob{},   // splits work into tasks
    TaskExecutor:      &invoiceExportTask{},  // processes one task
    Schedule:          "0 2 * * *",           // every day at 02:00
    JobCompletionCron: "*/5 * * * *",         // finalize completed jobs every 5 min
    Retry:             scherry.RetryConfig{MaxRetries: 3, RetryDelay: 10 * time.Minute},
    Kafka: scherry.KafkaConfig{
        Brokers:        []string{"localhost:9092"},
        TaskTopic:      "invoice_export_tasks",
        TaskRetryTopic: "invoice_export_tasks_retry",
        TaskDLQTopic:   "invoice_export_tasks_dlq",
        ConsumerGroup:  "invoice_export_worker",
    },
})
```

Asynq fires the job on the cron schedule. scherry splits it into tasks, persists state to Postgres, and publishes tasks to Kafka.

---

## Manual / on-demand jobs

**Use case:** Let users trigger work from an API — ad-hoc data export, one-off migration, or admin "run now" action.

```go
// Register once at startup — no Schedule field.
err := svc.RegisterManual(scherry.JobConfig{
    Name:              "user_data_export",
    JobExecutor:       &exportJob{},
    TaskExecutor:      &exportTask{},
    JobCompletionCron: "*/2 * * * *",
    Retry:             scherry.RetryConfig{MaxRetries: 5, RetryDelay: time.Minute},
    Kafka: scherry.KafkaConfig{
        Brokers:        []string{"localhost:9092"},
        TaskTopic:      "user_export_tasks",
        TaskRetryTopic: "user_export_tasks_retry",
        TaskDLQTopic:   "user_export_tasks_dlq",
        ConsumerGroup:  "user_export_worker",
    },
})

// Fire from an HTTP handler, event consumer, or CLI.
refID := uuid.New().String()
err = svc.Trigger(ctx, "user_data_export", refID, map[string]interface{}{
    "user_id": "usr_123",
    "format":  "csv",
})
```

The `metadata` map is passed to `JobExecutor.Execute` so the splitter can scope work to the caller's request. See [`examples/payroll/main.go`](examples/payroll/main.go) for a bonus payout triggered via `POST /bonus/trigger`.

---

## Job → task splitting

**Use case:** Turn one large job into thousands of parallel tasks — process every customer, every file in a bucket, or every row in a batch table.

```go
type payrollBatchJob struct {
    repo      EmployeeRepo
    batchSize int
}

func (j *payrollBatchJob) Execute(ctx context.Context, metadata map[string]interface{}) ([]scherry.TaskData, error) {
    employees, err := j.repo.ListActive(ctx)
    if err != nil {
        return nil, err
    }

    tasks := make([]scherry.TaskData, 0)
    for i, batch := range chunk(employees, j.batchSize) {
        ids := employeeIDs(batch)
        tasks = append(tasks, scherry.TaskData{
            Name:   "payroll_payout",
            Params: map[string]interface{}{"employee_ids": ids, "batch": i + 1},
        })
    }
    return tasks, nil
}
```

Each element in the returned slice becomes one persisted task and one Kafka message. Returning an empty slice finalizes the job immediately as `COMPLETED`.

---

## Task execution

**Use case:** Process a single unit of work — one API call, one file, one database batch — and optionally return a result payload stored in Postgres.

```go
type payrollPayoutTask struct {
    payments PaymentGateway
}

func (t *payrollPayoutTask) Execute(ctx context.Context, params map[string]interface{}) (map[string]interface{}, error) {
    ids, _ := params["employee_ids"].([]interface{})
    for _, raw := range ids {
        empID := raw.(string)
        if err := t.payments.TransferSalary(ctx, empID); err != nil {
            return nil, fmt.Errorf("transfer %s: %w", empID, err)
        }
    }
    return map[string]interface{}{"processed": len(ids)}, nil
}
```

Return an error to trigger the retry flow (if retries remain). The result map is persisted alongside the task record for auditing.

---

## Task distribution keys

**Use case:** Route related tasks to the same Kafka partition so they are processed in order by one consumer — per-customer serialization, per-account affinity, or batch ordering.

```go
tasks = append(tasks, scherry.TaskData{
    Name:   "notify_customer",
    Params: map[string]interface{}{"customer_id": cust.ID, "template": "invoice"},
    // All tasks for the same customer land on the same partition.
    DistributionKey: cust.ID,
})
```

| Scenario | DistributionKey | Why |
|---|---|---|
| Per-customer ordering | Customer or account ID | Avoid race conditions when updating the same account |
| Batch grouping | Batch identifier | Keep tasks within a batch ordered |
| Maximum parallelism | Leave empty (default) | Round-robin across partitions |

---

## Best-effort deduplication (refID)

**Use case:** Reduce duplicate job runs caused by a retried API call or webhook — payment reconciliation, webhook delivery, or form submission.

```go
// Use a stable business key, not a random UUID, when deduplication matters.
refID := fmt.Sprintf("order-%s-export", orderID)

err := svc.Trigger(ctx, "order_export", refID, map[string]interface{}{
    "order_id": orderID,
})
// A later Trigger with the same refID reuses the first run instead of starting a new one.
```

The caller supplies `refID`. Before creating a run, `ExecuteJob` looks for an existing job with the same `unique_reference_id` and reuses it if one is found. A trigger that is retried *after* the first one has been persisted — a network retry, a double-click, an at-least-once webhook redelivery — therefore reuses the original run.

### Idempotency limitations

This check is best-effort, not a guarantee. `ExecuteJob` performs the lookup and the insert as two separate statements, and `scherry_jobs.unique_reference_id` carries a plain index rather than a unique constraint. Two triggers with the same `refID` that overlap in time can both observe "no existing job" and both create a run.

A database-level constraint is not currently available to close this window. `scherry_jobs` is `PARTITION BY RANGE (created_at)`, and PostgreSQL requires a unique index on a partitioned table to include every partition key column. `UNIQUE (unique_reference_id)` therefore cannot be created, and `UNIQUE (unique_reference_id, created_at)` would permit exactly the duplicates it is meant to prevent.

Two consequences to design around:

- Duplicate `unique_reference_id` rows can exist. Lookups resolve them by returning the most recent match (`ORDER BY created_at DESC`).
- The History tab's reference ID filter can return more than one job for a single `refID`.

Treat `refID` as duplicate suppression for sequentially retried triggers rather than as an idempotency key. If a duplicate run would be harmful, make the effect itself idempotent inside your `JobExecutor` and `TaskExecutor` — a unique constraint on your own tables, a conditional update, or a lock keyed by the business ID.

---

## CancelPrevious

**Use case:** Only the latest run should matter — inventory sync, search index rebuild, or config propagation where stale in-flight work is harmful.

```go
err := svc.Register(scherry.JobConfig{
    Name:           "search_index_rebuild",
    CancelPrevious: true, // new run cancels any in-flight run of the same job
    Schedule:       "*/15 * * * *",
    // ... JobExecutor, TaskExecutor, Kafka, JobCompletionCron, etc.
})
```

When a new run starts, scherry cancels the previous `RUNNING` instance and its non-terminal tasks. The cancelled job moves to `CANCELLED`.

---

## Enabled callback

**Use case:** Gate jobs behind a feature flag, maintenance window, or environment check without unregistering them.

```go
err := svc.Register(scherry.JobConfig{
    Name:     "beta_feature_batch",
    Schedule: "0 * * * *",
    Enabled: func() bool {
        return os.Getenv("BETA_BATCH_ENABLED") == "true"
    },
    // ... JobExecutor, TaskExecutor, Kafka, JobCompletionCron, etc.
})
```

When `Enabled()` returns `false` at execution time, the run is rejected with status `REJECTED` and no tasks are created.

---

## Retry policy and DLQ

**Use case:** Handle transient downstream failures (rate limits, network blips) with delayed retries, and route permanently failed tasks to a DLQ for inspection.

```go
Retry: scherry.RetryConfig{
    MaxRetries: 5,
    RetryDelay: 3 * time.Minute, // wait before re-publishing to the retry topic
},
Kafka: scherry.KafkaConfig{
    TaskTopic:      "payment_tasks",
    TaskRetryTopic: "payment_tasks_retry",  // required for automatic retries
    TaskDLQTopic:   "payment_tasks_dlq",    // tasks land here after MaxRetries
    ConsumerGroup:  "payment_worker",
    Brokers:        []string{"localhost:9092"},
},
```

**Flow:**

1. Task fails → status `FAILED`
2. Retries remaining → reset to `PENDING`, re-published to `TaskRetryTopic` after `RetryDelay`
3. No retries left → status `MAX_RETRIES_EXHAUSTED`, published to `TaskDLQTopic`

Inspect DLQ messages with your Kafka tooling or replay them manually after fixing the root cause.

---

## Programmatic job retry

**Use case:** Re-run failed tasks from your admin API or the history console — partial retry (failed only) or full retry (every task).

```go
// Partial retry — only MAX_RETRIES_EXHAUSTED tasks are re-queued.
newJob, err := svc.RetryJob(ctx, originalJobID, false, uuid.New().String())

// Full retry — every task from the original job is re-run.
newJob, err := svc.RetryJob(ctx, originalJobID, true, uuid.New().String())
```

The original job must be in a terminal state (`COMPLETED`, `PARTIAL_FAILED`, `FAILED`, etc.). The new job links back via `parent_job_id` for retry lineage in the console.

---

## Job completion checks

**Use case:** Finalize jobs on your own schedule — admin "finalize now" button, integration test teardown, or a custom scheduler instead of (or in addition to) `JobCompletionCron`.

```go
// JobCompletionCron on JobConfig is still required at registration.
// It runs this same logic on a schedule; you can also call it directly:
err := svc.CheckJobCompletions(ctx, "nightly_invoice_export")
```

scherry finalizes any `RUNNING` job whose tasks have all reached a terminal state (`COMPLETED`, `MAX_RETRIES_EXHAUSTED`, `CANCELLED`, `REJECTED`).

---

## Shared Kafka consumer

**Use case:** Pool Kafka resources across related low-volume jobs, or cap concurrent load on a shared downstream (rate-limited API, single database connection pool).

```go
err := svc.RegisterShared(scherry.SharedKafkaConfig{
    Kafka: scherry.KafkaConfig{
        Brokers:        []string{"localhost:9092"},
        TaskTopic:      "billing_tasks",
        TaskRetryTopic: "billing_tasks_retry",
        TaskDLQTopic:   "billing_tasks_dlq",
        ConsumerGroup:  "billing_shared_worker",
    },
    TaskExecutor: &billingTaskExecutor{}, // one executor for all coupled jobs
    Jobs: []scherry.JobConfig{
        {
            Name:              "monthly_invoice_job",
            Schedule:          "0 0 1 * *",
            JobExecutor:       &monthlyInvoiceJob{},
            JobCompletionCron: "*/5 * * * *",
            Retry:             scherry.RetryConfig{MaxRetries: 3, RetryDelay: 5 * time.Minute},
        },
        {
            Name:              "adhoc_refund_job", // manual — no Schedule
            JobExecutor:       &refundJob{},
            JobCompletionCron: "*/5 * * * *",
            Retry:             scherry.RetryConfig{MaxRetries: 5, RetryDelay: time.Minute},
        },
    },
})
```

Requires at least two jobs. Kafka config and `TaskExecutor` live on `SharedKafkaConfig`; each job keeps its own `JobExecutor`, schedule, retry policy, and history.

**When to use:** Low-volume related jobs sharing a rate-limited downstream. **When not to:** Jobs that need independent consumer scaling or isolation.

---

## Kafka tuning

**Use case:** Optimize throughput, latency, or consumer behavior for high-volume or latency-sensitive workloads.

### Consumer tuning

```go
import kafkago "github.com/segmentio/kafka-go"

ConsumerConfig: kafkago.ReaderConfig{
    MinBytes:       10_000,
    MaxBytes:       10_000_000,
    MaxWait:        250 * time.Millisecond,
    SessionTimeout: 45 * time.Second,
    CommitInterval: time.Second,
    StartOffset:    kafkago.LastOffset, // skip backlog for new consumer groups
},
```

### Producer tuning

```go
WriterConfig: &kafkago.Writer{
    Balancer:     &kafkago.Hash{},           // required for DistributionKey routing
    RequiredAcks: kafkago.RequireAll,
    Compression:  kafkago.Zstd,
    BatchSize:    500,
    BatchTimeout: 10 * time.Millisecond,
},
```

See [`examples/payroll/main.go`](examples/payroll/main.go) for a working `ConsumerConfig` / `WriterConfig` on the bonus job.

---

## TLS and SASL

**Use case:** Connect to managed Kafka (Confluent Cloud, AWS MSK, Azure Event Hubs).

```go
import (
    kafkago "github.com/segmentio/kafka-go"
    "github.com/segmentio/kafka-go/sasl/plain"
)

Kafka: scherry.KafkaConfig{
    Brokers: []string{"pkc-xxxxx.region.provider.com:9092"},
    // ... topics and consumer group ...
    ConsumerConfig: kafkago.ReaderConfig{
        Dialer: &kafkago.Dialer{
            TLS:           tlsConfig,
            SASLMechanism: plain.Mechanism{Username: apiKey, Password: apiSecret},
        },
    },
    WriterConfig: &kafkago.Writer{
        Transport: &kafkago.Transport{
            TLS:  tlsConfig,
            SASL: plain.Mechanism{Username: apiKey, Password: apiSecret},
        },
    },
},
```

---

## History console

**Use case:** Inspect job runs, task progress, retry lineage, and trigger retries without writing custom SQL or dashboards.

### Standalone server

```go
consoleServer, err := svc.StartConsole(":3002")
if err != nil {
    log.Fatal(err)
}
defer consoleServer.Shutdown(context.Background())
// Open http://localhost:3002
```

### Behind your own HTTP server (with auth)

```go
mux := http.NewServeMux()
mux.Handle("/admin/jobs/", http.StripPrefix("/admin/jobs", svc.ConsoleHandler()))
// Protect with your auth middleware before exposing in production.
```

**Console tabs:**

| Tab | Use case |
|---|---|
| **Overview** | Aggregate success/error rates and task throughput over time |
| **History** | Drill into a specific run, view task results, trigger partial/full retry |
| **Jobs** | Verify registered jobs, Kafka topics, and retry settings |
| **Upcoming** | See next scheduled fires for cron jobs |

> The console has no built-in authentication. Use localhost in development and protect it behind auth and network controls in production.

---

## Execution hooks

**Use case:** Emit Prometheus metrics, structured audit logs, or alerts when tasks and jobs complete.

```go
hooks := scherry.Hooks{
    OnTasksPublished: func(ctx context.Context, jobName string, count int) {
        tasksPublished.WithLabelValues(jobName).Add(float64(count))
    },
    OnTaskStarted: func(ctx context.Context, jobName, taskID string, attempt int) {
        tasksInFlight.WithLabelValues(jobName).Inc()
    },
    OnTaskFinished: func(ctx context.Context, jobName, taskID, status string, d time.Duration) {
        tasksInFlight.WithLabelValues(jobName).Dec()
        taskDuration.WithLabelValues(jobName, status).Observe(d.Seconds())
    },
    OnJobFinished: func(ctx context.Context, jobName, refID, status string, d time.Duration) {
        jobDuration.WithLabelValues(jobName, status).Observe(d.Seconds())
    },
}

svc, _ := scherry.New(scherry.Config{
    // ...
    Hooks: hooks,
})
```

Per-job hooks in `JobConfig.Hooks` override the service-level hooks for that job. Unset hooks are no-ops (`scherry.NopHooks`).

---

## Programmatic task queries

**Use case:** Build your own status API, progress bar, or notification without hitting the console HTTP API.

```go
import "github.com/google/uuid"

page, err := svc.GetJobTasks(ctx, scherry.TaskFilter{
    JobID:  jobUUID,
    Status: scherry.TaskStatusCompleted, // optional filter
    Limit:  50,
    Offset: 0,
})
// page.Tasks — ordered by sequence_number (generation order)
// page.Total — total matching tasks
```

Use `sequence_number` (not `created_at`) for generation order. Tasks are returned in the order `JobExecutor.Execute` produced them.

---

## Custom transport

**Use case:** Swap Kafka for an in-memory broker in tests, or integrate a custom message bus while keeping scherry's orchestration.

```go
// Inject a custom publisher when registering a job.
err := svc.Register(scherry.JobConfig{
    Name:         "my_job",
    Publisher:    myCustomPublisher,  // implements scherry.Publisher
    TaskReader:   myCustomReader,     // implements scherry.MessageReader
    RetryReader:  myRetryReader,
    // ... JobExecutor, TaskExecutor, Kafka (routing fields still required), etc.
})
```

`Publisher` must implement `PublishTasks` and `PublishRetryTasks`. `MessageReader` must implement `FetchMessage`, `CommitMessages`, and `Close`.

For shared consumers, inject `Publisher`, `TaskReader`, and `RetryReader` on `SharedKafkaConfig` instead of per-job config.

---

## Logging

**Use case:** Plug scherry into your existing structured logging stack.

```go
// Built-in slog adapter.
logger := scherry.NewSlogLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

// Or implement scherry.Logger yourself.
type myLogger struct { /* ... */ }
func (l *myLogger) Debug(msg string, args ...interface{}) { /* ... */ }
func (l *myLogger) Info(msg string, args ...interface{})  { /* ... */ }
func (l *myLogger) Warn(msg string, args ...interface{})  { /* ... */ }
func (l *myLogger) Error(msg string, args ...interface{}) { /* ... */ }
```

Logger is required in `scherry.Config`.

---

## Async job retry via Asynq

**Use case:** Queue a job retry as a background Asynq task instead of blocking the HTTP handler that initiated it.

```go
mux := asynq.NewServeMux()
svc.RegisterRetryHandler(mux, "scherry:retry-job")

// Enqueue from your admin API:
payload, _ := json.Marshal(scherry.RetryJobPayload{
    JobID:     jobID,
    FullRetry: false,
    RefID:     uuid.New().String(),
})
client.Enqueue(asynq.NewTask("scherry:retry-job", payload))
```

`RegisterRetryHandler` wires the Asynq task type to `svc.RetryJob`.

---

## Quick reference: feature → use case

| Feature | Typical use case |
|---|---|
| `Register` | Recurring batch jobs (exports, syncs, reports) |
| `RegisterManual` + `Trigger` | User-initiated or event-driven work |
| `JobExecutor` | Split one job into N parallel tasks |
| `TaskExecutor` | Process one task unit |
| `DistributionKey` | Per-customer ordering or consumer affinity |
| `refID` | Best-effort duplicate suppression for triggers from APIs and webhooks |
| `CancelPrevious` | Only-latest-run jobs (index rebuilds, syncs) |
| `Enabled` | Feature flags and maintenance gates |
| `Retry` + retry/DLQ topics | Transient failure handling |
| `RetryJob` | Re-run failed or all tasks after fixing an issue |
| `CheckJobCompletions` | On-demand or custom finalization |
| `RegisterShared` | Pool Kafka consumers across related jobs |
| `ConsumerConfig` / `WriterConfig` | Throughput, latency, and broker tuning |
| `StartConsole` / `ConsoleHandler` | Ops visibility and one-click retry |
| `Hooks` | Metrics, tracing, and audit events |
| `GetJobTasks` | Custom progress APIs |
| Custom `Publisher` / `MessageReader` | Tests or alternate brokers |

---

## See also

- [README.md](README.md) — architecture, lifecycle, and full API summary
- [`examples/payroll/`](examples/payroll/) — runnable demo with scheduled payroll and on-demand bonus jobs
- [`migrations/`](migrations/) — Postgres schema for `scherry_jobs` and `scherry_tasks`
