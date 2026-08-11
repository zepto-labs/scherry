# Security Policy


## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security vulnerability, please report it by emailing:

**opensource@zepto.com**

Include as much detail as possible:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Any suggested mitigations (optional)

You can expect an acknowledgement within **48 hours** and a resolution timeline within **7 days** for critical issues.

We appreciate responsible disclosure and will credit reporters in the release notes unless you prefer to remain anonymous.

## Security considerations

`scherry` is a library that you embed in your own service and wire up to your own PostgreSQL, Redis, and Kafka infrastructure. Understanding where its trust boundaries lie is important for deploying it safely.

### History console has no built-in authentication

The built-in history console (`ConsoleHandler` / `StartConsole`) exposes job and task history, including any metadata, task parameters, and result payloads persisted to Postgres. It ships with **no authentication or authorization**.

- Serve it on `localhost` during development.
- In production, mount `ConsoleHandler()` behind your own authentication middleware and/or restrict access with network controls (VPN, private subnet, firewall rules).
- Treat the console's JSON API endpoints as sensitive — they return the same data as the UI.

### Infrastructure credentials and transport security

- Connection details for PostgreSQL, Redis, and Kafka are supplied by your application at bootstrap. Keep these credentials out of source control and inject them via your secret-management tooling.
- For Kafka, enable TLS and SASL through the native kafka-go fields on `ConsumerConfig` and `WriterConfig` (see the "TLS and SASL" section of the README) when connecting to managed brokers such as Confluent Cloud or AWS MSK.
- Ensure Redis and PostgreSQL connections use TLS and authentication appropriate for your environment.

### Sensitive data in job and task payloads

Job `metadata`, `TaskData.Params`, and task result payloads are persisted durably to the `scherry_jobs` / `scherry_tasks` tables and rendered in the console. Avoid placing secrets (passwords, tokens, PII you do not need) in these payloads; store references or identifiers instead and resolve them at execution time.
