# Payroll Example

This example demonstrates two jobs running side-by-side in one service:

| Job | Registration | Trigger |
|-----|-------------|---------|
| `monthly_payroll_job` | `Register` (cron `* * * * *`) | Asynq scheduler fires it automatically |
| `bonus_payout_job` | `RegisterManual` (no schedule) | `POST /bonus/trigger` HTTP endpoint |

## Flows

### Monthly payroll (scheduled)

1. Asynq cron fires `monthly_payroll_job` every minute (demo cadence)
2. `payrollBatchJobExecutor` loads employees and splits them into batches
3. Tasks are persisted and published to Kafka
4. `payrollPayoutTaskExecutor` processes each batch (simulated salary transfer)

### Bonus payout (manual trigger)

```
POST /bonus/trigger  {"bonus_pct": 15}
        │
        ▼
svc.Trigger(ctx, "bonus_payout_job", refID, {"bonus_pct": 15})
        │
        ▼
bonusBatchJobExecutor.Execute   ← reads bonus_pct from metadata, batches employees
        │  stamps bonus_pct into each task's params
        ▼
Kafka → bonusPayoutTaskExecutor.Execute  ← applies bonus_pct per employee
```

## Quick start

Requires [Docker](https://docs.docker.com/get-docker/) and Go 1.23+.

```bash
# 1. Start Postgres, Redis, and Kafka (applies all migrations automatically)
cd examples/payroll
docker compose up -d --wait

# 2. Run the example (defaults match docker-compose)
go run .

# 3. Trigger a 15% bonus payout via the API
curl -s -X POST http://localhost:8080/bonus/trigger \
  -H "Content-Type: application/json" \
  -d '{"bonus_pct": 15}' | jq

# 4. Check simulated payout records
curl -s http://localhost:8080/bonus/payouts | jq

# 5. Open the history console — both jobs appear here
open http://localhost:3002
```

Stop infrastructure when done:

```bash
docker compose down
```

Remove Postgres data as well:

```bash
docker compose down -v
```

## Bonus trigger API

### `POST /bonus/trigger`

Fires a `bonus_payout_job` run. Each call generates a fresh random `ref_id`, so every request starts a new run. To have a retried request reuse the existing run instead, pass a stable business key as the `ref_id` — note that this deduplication is best-effort and does not protect against two overlapping requests.

**Request body:**
```json
{ "bonus_pct": 15.0 }
```

**Response (`202 Accepted`):**
```json
{
  "bonus_pct": 15,
  "job": "bonus_payout_job",
  "ref_id": "3f2c8a1e-..."
}
```

### `GET /bonus/payouts`

Returns all simulated payout and bonus transfer records for verification.

## Environment variables

Defaults match `docker-compose.yml`. Override only if your services run elsewhere.

| Variable       | Default           |
|----------------|-------------------|
| `REDIS_ADDR`   | `127.0.0.1:6379`  |
| `KAFKA_BROKERS`| `127.0.0.1:9092`  |
| `PG_HOST`      | `127.0.0.1`       |
| `PG_USER`      | `postgres`        |
| `PG_PASSWORD`  | `postgres`        |
| `PG_DB`        | `scherry`    |
| `API_ADDR`     | `:8080`           |
| `CONSOLE_ADDR` | `:3002`           |

## Manual setup (without Docker)

If you already have Postgres, Redis, and Kafka running locally, apply migrations from [`../../migrations/`](../../migrations/) then run from the repository root:

```bash
go run ./examples/payroll
```

## Mapping to production

| Library piece         | Payroll equivalent                                      | Bonus equivalent                                        |
|-----------------------|---------------------------------------------------------|---------------------------------------------------------|
| `Register`            | Monthly cron job                                        | —                                                       |
| `RegisterManual`      | —                                                       | No schedule; only fires when the API is called          |
| `svc.Trigger`         | —                                                       | Called from the HTTP handler with `bonus_pct` in metadata |
| `JobExecutor.Execute` | Load employees, batch by N                              | Read `bonus_pct` from metadata, batch employees         |
| `TaskExecutor.Execute`| Transfer salary per employee in batch                   | Compute and transfer bonus per employee in batch        |
| `TaskData.DistributionKey` | Employee IDs joined with `_` — routes each batch to the same Kafka partition | Same |
| `JobConfig.Kafka`     | Task/retry/DLQ topics for payroll workers               | Task/retry/DLQ topics for bonus workers                 |
| `Hooks`               | Metrics on tasks published / payout duration            | Same                                                    |

Payouts and bonuses are simulated in memory — no real payment gateway is used.
