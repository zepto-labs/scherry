# Contributing

Contributions are always welcome, no matter how large or small!

We want this community to be friendly and respectful to each other. Please follow it in all your interactions with the project. Before contributing, please read the [code of conduct](./CODE_OF_CONDUCT.md).

## Development workflow

`scherry` is a Go library (module `github.com/zepto-labs/scherry`). This repository contains:

- The library package in the root directory and under `internal/`.
- A runnable [payroll example](/examples/payroll/) in the `examples/` directory.

To get started, make sure you have [Go](https://go.dev/dl/) installed (see the version in [`go.mod`](./go.mod)), then download the dependencies:

```sh
go mod download
```

Build the library to make sure everything compiles:

```sh
go build ./...
```

The [payroll example](/examples/payroll/) demonstrates usage of the library. You need to run it to exercise any changes you make end-to-end. It uses Docker Compose to bring up PostgreSQL, Redis, and Kafka and applies the database migrations automatically:

```sh
cd examples/payroll
docker compose up -d --wait
go run .
```

The history console is then available at [http://localhost:3002](http://localhost:3002). See [`examples/payroll/README.md`](/examples/payroll/README.md) for the full walkthrough.

### Commit message convention

We follow the [conventional commits specification](https://www.conventionalcommits.org/en) for our commit messages:

- `fix`: bug fixes, e.g. fix a race in the task consumer.
- `feat`: new features, e.g. add a new option to `JobConfig`.
- `refactor`: code refactor, e.g. extract the FSM into its own package.
- `docs`: changes into documentation, e.g. add a usage example to the README.
- `test`: adding or updating tests, e.g. add coverage for the retry flow.
- `chore`: tooling changes, e.g. change CI config.

### Linting and tests

We use the standard Go toolchain: [`gofmt`](https://pkg.go.dev/cmd/gofmt) for formatting and [`go vet`](https://pkg.go.dev/cmd/vet) for static analysis.

Format and vet the code before sending a change:

```sh
gofmt -l -w .
go vet ./...
```

Run the test suite:

```sh
go test ./...
```

CI runs the tests under `./internal/...` with coverage via [gotestsum](https://github.com/gotestlabs/gotestsum) and publishes a coverage report on each pull request. To reproduce the coverage run locally:

```sh
go test -coverprofile=coverage.out ./internal/...
go tool cover -func=coverage.out
```

### Publishing a release

Releases are published as Go modules via git tags following [Semantic Versioning](https://semver.org/):

```sh
git tag v1.2.3
git push origin v1.2.3
```

Consumers then pick up the new version with `go get github.com/zepto-labs/scherry@v1.2.3`. Remember to update the [CHANGELOG](./CHANGELOG.md) as part of the release.

### Common tasks

- `go mod download`: install dependencies.
- `go build ./...`: build the library and examples.
- `go vet ./...`: run static analysis.
- `go test ./...`: run the test suite.
- `cd examples/payroll && docker compose up -d --wait && go run .`: run the example app.

### Sending a pull request

> **Working on your first pull request?** You can learn how from this _free_ series: [How to Contribute to an Open Source Project on GitHub](https://app.egghead.io/playlists/how-to-contribute-to-an-open-source-project-on-github).

When you're sending a pull request:

- Prefer small pull requests focused on one change.
- Verify that the code builds, `go vet ./...` is clean, and `go test ./...` passes.
- Review the documentation to make sure it looks good.
- For pull requests that change the API or implementation, discuss with maintainers first by opening an issue.
