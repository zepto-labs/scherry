# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Module path updated from `github.com/zeptonow/scherry` to `github.com/zepto-labs/scherry` for public release under the zepto-labs organization.
- GitHub Actions workflow updated for public repository CI on `zepto-labs-migration`.
- CODEOWNERS updated to `@zepto-labs/scherry-maintainers`.

### Added

- `docs/CONFIGURATION.md` with bootstrap, job, shared consumer, and example env var reference.
- GitHub issue templates (bug report and feature request) and pull request template.
- `examples/payroll/.env.example` for local development.

## [1.0.0] - 2026-07-03

### Added

- Initial open-source release of `github.com/zepto-labs/scherry`.
- Two-level job → task model built on [Asynq](https://github.com/hibiken/asynq), with Kafka fan-out and durable PostgreSQL persistence.
- Cron-scheduled jobs (`Register`) and manual / on-demand jobs (`RegisterManual` + `Trigger`).
- Task retries via a dedicated Kafka retry topic and a dead-letter queue (DLQ) for exhausted tasks.
- FSM-based job and task lifecycle with automatic job finalization.
- Idempotent execution via a caller-supplied `refID`.
- Built-in Postgres-backed history console web UI.
- Execution hooks (`OnTasksPublished`, `OnTaskStarted`, `OnTaskFinished`, `OnJobFinished`) for custom instrumentation.
