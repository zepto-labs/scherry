# Configuration reference

`scherry` is configured entirely through Go structs at bootstrap and job registration time. The library does **not** read environment variables directly; your application (or the [payroll example](../examples/payroll/)) is responsible for loading settings from env files, secret managers, or config services and mapping them into these structs.

For a runnable reference, see [`examples/payroll/main.go`](../examples/payroll/main.go) and [`examples/payroll/.env.example`](../examples/payroll/.env.example).

## Bootstrap: `scherry.Config`

Passed to `scherry.New`.

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `ServiceName` | `string` | — | **Yes** | Logical name for this scheduler instance. |
| `Logger` | `Logger` | — | **Yes** | Structured logger (`scherry.NewSlogLogger` or custom). |
| `Redis` | `RedisConfig` | — | **Yes** | Redis connection used by Asynq for cron triggers. |
| `Database` | `DatabaseConfig` | — | **Yes**† | PostgreSQL settings when `DB` is not provided. |
| `DB` | `*gorm.DB` | `nil` | No | Pre-opened GORM handle; when set, `Database` validation is skipped. |

† Either `Database` or `DB` must be supplied.

### `RedisConfig`

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `Addr` | `string` | — | **Yes** | Redis address (`host:port`). |
| `Password` | `string` | `""` | No | Redis password. |
| `DB` | `int` | `0` | No | Redis database index (must be ≥ 0). |

### `DatabaseConfig`

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `Host` | `string` | — | **Yes** | PostgreSQL host. |
| `Port` | `int` | — | **Yes** | PostgreSQL port (> 0). |
| `User` | `string` | — | **Yes** | PostgreSQL user. |
| `Password` | `string` | `""` | No | PostgreSQL password. |
| `DBName` | `string` | — | **Yes** | Database name. |
| `SSLMode` | `string` | `"disable"` | No | DSN `sslmode`; empty becomes `"disable"`. |
| `MaxOpenConns` | `int` | `25` | No | Connection pool maximum; ≤ 0 becomes `25`. |
| `MaxIdleConns` | `int` | `5` | No | Connection pool minimum; ≤ 0 becomes `5`. |

## Per-job: `scherry.JobConfig`

Passed to `Register`, `RegisterManual`, or as entries in `SharedKafkaConfig.Jobs`.

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `Name` | `string` | — | **Yes** | Unique job name. |
| `JobExecutor` | `JobExecutor` | — | **Yes** | Splits a run into `[]TaskData`. |
| `TaskExecutor` | `TaskExecutor` | — | **Yes**‡ | Executes individual tasks. |
| `Kafka` | `KafkaConfig` | — | **Yes**‡ | Kafka routing and tuning. |
| `Schedule` | `string` | `""` | **Yes** for `Register` | Cron expression for Asynq. Omit for manual jobs. |
| `JobCompletionCron` | `string` | — | **Yes** | Cron for periodic job finalization checks. |
| `Retry` | `RetryConfig` | see below | No | Task retry policy. |
| `AsynqOptions` | `[]asynq.Option` | `nil` | No | Asynq task options (retention, max retry, etc.). |
| `CancelPrevious` | `bool` | `false` | No | Cancel in-flight runs when a new run starts. |
| `Enabled` | `func() bool` | `nil` | No | Feature flag; `nil` means enabled. |
| `Hooks` | `Hooks` | no-ops | No | Lifecycle callbacks. |
| `Publisher` | `Publisher` | built-in | No | Custom task publisher. |
| `TaskReader` | `MessageReader` | built-in | No | Custom main Kafka consumer. |
| `RetryReader` | `MessageReader` | built-in | No | Custom retry Kafka consumer. |

‡ For `RegisterShared`, set `TaskExecutor` and `Kafka` on the shared config instead; they must be empty on each job entry.

### `KafkaConfig`

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `Brokers` | `[]string` | — | **Yes** | Kafka broker addresses. |
| `TaskTopic` | `string` | — | **Yes** | Main task topic. |
| `TaskRetryTopic` | `string` | `""` | No | Retry topic; empty disables retry consumer. |
| `TaskDLQTopic` | `string` | `""` | No | DLQ topic; empty discards exhausted tasks. |
| `ConsumerGroup` | `string` | — | **Yes** | Main consumer group ID. |
| `ConsumerConfig` | `kafkago.ReaderConfig` | kafka-go defaults | No | Reader tuning; `Brokers`, `GroupID`, `Topic` are set by the library. |
| `WriterConfig` | `*kafkago.Writer` | Hash balancer | No | Producer tuning; `Addr` is set from `Brokers` when nil. |

The retry consumer group is `{ConsumerGroup}-retry`.

### `RetryConfig`

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `MaxRetries` | `int` | `3` | No | Max task attempts before DLQ; `0` becomes `3`. |
| `RetryDelay` | `time.Duration` | `0` | No | Minimum wait before a retried task is consumed again. |

## Shared consumer: `scherry.SharedKafkaConfig`

Passed to `RegisterShared` when two or more jobs share one Kafka consumer.

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `Kafka` | `KafkaConfig` | — | **Yes** | Shared Kafka routing and tuning. |
| `TaskExecutor` | `TaskExecutor` | — | **Yes** | Shared task processor for all coupled jobs. |
| `Jobs` | `[]JobConfig` | — | **Yes** (≥ 2) | Coupled jobs; each must set `Name`, `JobExecutor`, and `JobCompletionCron`. |
| `Publisher` | `Publisher` | built-in | No | Shared publisher override. |
| `TaskReader` | `MessageReader` | built-in | No | Shared main consumer override. |
| `RetryReader` | `MessageReader` | built-in | No | Shared retry consumer override. |

## Console

The history console is started separately:

```go
consoleServer, err := svc.StartConsole(":3002")
```

Or mount `svc.ConsoleHandler()` behind your own HTTP mux. The console has **no built-in authentication**; restrict network access in production.

## Example environment variables

The payroll example reads these variables (see [`.env.example`](../examples/payroll/.env.example)):

| Variable | Maps to | Default | Required |
|---|---|---|---|
| `REDIS_ADDR` | `RedisConfig.Addr` | `127.0.0.1:6379` | No |
| `PG_HOST` | `DatabaseConfig.Host` | `127.0.0.1` | No |
| `PG_USER` | `DatabaseConfig.User` | `postgres` | No |
| `PG_PASSWORD` | `DatabaseConfig.Password` | `postgres` | No |
| `PG_DB` | `DatabaseConfig.DBName` | `jobscheduler` | No |
| `KAFKA_BROKERS` | `KafkaConfig.Brokers[0]` | `127.0.0.1:9092` | No |
| `API_ADDR` | Bonus HTTP API listen address | `:8080` | No |
| `CONSOLE_ADDR` | Console listen address | `:3002` | No |

## Security notes

- Keep PostgreSQL, Redis, and Kafka credentials out of source control.
- Enable TLS and SASL on Kafka when connecting to managed brokers (see README “TLS and SASL”).
- Avoid placing secrets or unnecessary PII in job metadata or task params; they are persisted and shown in the console.
