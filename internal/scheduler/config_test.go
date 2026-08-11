package scheduler

import (
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/logging"
)

func TestRedisConfigAsynqClientOpt(t *testing.T) {
	cfg := RedisConfig{Addr: "localhost:6379", Password: "secret", DB: 2}
	opt := cfg.AsynqClientOpt()

	assert.Equal(t, asynq.RedisClientOpt{Addr: "localhost:6379", Password: "secret", DB: 2}, opt)
}

func TestBuildDSN(t *testing.T) {
	dsn := buildDSN(DatabaseConfig{
		Host: "db.example.com", Port: 5432, User: "svc", Password: "pw", DBName: "scheduler",
	})

	// buildDSN forces host to "localhost" while running under `go test`
	// (testing.Testing() == true), regardless of the configured host.
	assert.Contains(t, dsn, "host=localhost")
	assert.Contains(t, dsn, "user=svc")
	assert.Contains(t, dsn, "password=pw")
	assert.Contains(t, dsn, "dbname=scheduler")
	assert.Contains(t, dsn, "port=5432")
	assert.Contains(t, dsn, "sslmode=disable")
	assert.Contains(t, dsn, "pool_max_conns=25")
	assert.Contains(t, dsn, "pool_min_conns=5")
}

func TestBuildDSNDefaultsAndOverrides(t *testing.T) {
	dsn := buildDSN(DatabaseConfig{
		Host: "db", Port: 5432, User: "u", DBName: "d",
		SSLMode: "require", MaxOpenConns: 100, MaxIdleConns: 10,
	})

	assert.Contains(t, dsn, "sslmode=require")
	assert.Contains(t, dsn, "pool_max_conns=100")
	assert.Contains(t, dsn, "pool_min_conns=10")
}

func TestValidateDatabaseConfig(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		err := validateDatabaseConfig(DatabaseConfig{Host: "h", Port: 5432, User: "u", DBName: "d"})
		assert.NoError(t, err)
	})

	t.Run("collects every missing field", func(t *testing.T) {
		err := validateDatabaseConfig(DatabaseConfig{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "host is required")
		assert.Contains(t, err.Error(), "port must be greater than 0")
		assert.Contains(t, err.Error(), "user is required")
		assert.Contains(t, err.Error(), "database name is required")
	})

	t.Run("negative port", func(t *testing.T) {
		err := validateDatabaseConfig(DatabaseConfig{Host: "h", Port: -1, User: "u", DBName: "d"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "port must be greater than 0")
	})
}

func TestValidateConfigBranches(t *testing.T) {
	base := Config{
		ServiceName: "svc",
		Redis:       RedisConfig{Addr: "localhost:6379"},
		Logger:      logging.NopLogger{},
		Database:    DatabaseConfig{Host: "localhost", Port: 5432, User: "u", DBName: "db"},
	}

	t.Run("missing service name", func(t *testing.T) {
		cfg := base
		cfg.ServiceName = "  "
		err := validateConfig(cfg)
		assert.ErrorContains(t, err, "service name is required")
	})

	t.Run("missing redis address", func(t *testing.T) {
		cfg := base
		cfg.Redis = RedisConfig{}
		err := validateConfig(cfg)
		assert.ErrorContains(t, err, "redis address is required")
	})

	t.Run("negative redis db", func(t *testing.T) {
		cfg := base
		cfg.Redis.DB = -1
		err := validateConfig(cfg)
		assert.ErrorContains(t, err, "redis DB cannot be negative")
	})

	t.Run("missing logger", func(t *testing.T) {
		cfg := base
		cfg.Logger = nil
		err := validateConfig(cfg)
		assert.ErrorContains(t, err, "logger is required")
	})

	t.Run("invalid database config wraps error", func(t *testing.T) {
		cfg := base
		cfg.Database = DatabaseConfig{}
		err := validateConfig(cfg)
		assert.ErrorContains(t, err, "database config")
		assert.ErrorContains(t, err, "host is required")
	})

	t.Run("skips database validation when DB already provided", func(t *testing.T) {
		cfg := base
		cfg.Database = DatabaseConfig{} // would fail validation on its own
		cfg.DB = &gorm.DB{}
		err := validateConfig(cfg)
		assert.NoError(t, err)
	})

	t.Run("valid", func(t *testing.T) {
		assert.NoError(t, validateConfig(base))
	})
}

func TestValidateConfig(t *testing.T) {
	err := validateConfig(Config{})
	assert.Error(t, err)

	err = validateConfig(Config{
		ServiceName: "svc",
		Redis:       RedisConfig{Addr: "localhost:6379"},
		Logger:      logging.NopLogger{},
		Database:    DatabaseConfig{Host: "localhost", Port: 5432, User: "u", DBName: "db"},
	})
	assert.NoError(t, err)
}
