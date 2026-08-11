// Package scheduler is the orchestration core: it defines Service, the
// bootstrap Config, and the job registration/lifecycle methods (Register,
// Start, Stop, ...). Execution logic lives in internal/executor, Kafka
// plumbing in internal/transport, and the HTTP console in internal/console;
// Service wires all three together and exposes thin delegating methods so
// callers keep a single entry point.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"

	"github.com/zepto-labs/scherry/internal/logging"
)

type Config struct {
	ServiceName string
	Database    DatabaseConfig
	Redis       RedisConfig
	Logger      logging.Logger
	DB          *gorm.DB
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

func (r RedisConfig) AsynqClientOpt() asynq.RedisClientOpt {
	return asynq.RedisClientOpt{
		Addr:     r.Addr,
		Password: r.Password,
		DB:       r.DB,
	}
}

type DatabaseConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	DBName       string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.ServiceName) == "" {
		return errors.New("service name is required")
	}
	if strings.TrimSpace(cfg.Redis.Addr) == "" {
		return errors.New("redis address is required")
	}
	if cfg.Redis.DB < 0 {
		return errors.New("redis DB cannot be negative")
	}
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.DB == nil {
		if err := validateDatabaseConfig(cfg.Database); err != nil {
			return fmt.Errorf("database config: %w", err)
		}
	}
	return nil
}

func validateDatabaseConfig(db DatabaseConfig) error {
	var errs []string
	if strings.TrimSpace(db.Host) == "" {
		errs = append(errs, "host is required")
	}
	if db.Port <= 0 {
		errs = append(errs, "port must be greater than 0")
	}
	if strings.TrimSpace(db.User) == "" {
		errs = append(errs, "user is required")
	}
	if strings.TrimSpace(db.DBName) == "" {
		errs = append(errs, "database name is required")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

func openDatabase(dbConfig DatabaseConfig) (*gorm.DB, error) {
	dsn := buildDSN(dbConfig)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Silent),
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return db, nil
}

func buildDSN(dbConfig DatabaseConfig) string {
	host := dbConfig.Host
	if testing.Testing() {
		host = "localhost"
	}

	sslMode := dbConfig.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	maxOpen := dbConfig.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 25
	}

	maxIdle := dbConfig.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 5
	}

	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=UTC pool_max_conns=%d pool_min_conns=%d",
		host, dbConfig.User, dbConfig.Password, dbConfig.DBName, dbConfig.Port, sslMode, maxOpen, maxIdle,
	)
}
