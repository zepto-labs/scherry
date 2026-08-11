package logging

import (
	"context"
	"log/slog"
)

type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	With(args ...any) Logger
	WithContext(ctx context.Context) Logger
}

type slogLogger struct {
	logger *slog.Logger
}

func NewSlogLogger(logger *slog.Logger) Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &slogLogger{logger: logger}
}

func (l *slogLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

func (l *slogLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

func (l *slogLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

func (l *slogLogger) With(args ...any) Logger {
	return &slogLogger{logger: l.logger.With(args...)}
}

func (l *slogLogger) WithContext(ctx context.Context) Logger {
	return &slogLogger{logger: l.logger.With("context", ctx)}
}

// NopLogger is a Logger implementation whose methods are all no-ops. It is
// exported so it can be used as a default/test logger from other packages
// (e.g. the root scherry package aliases it for internal use).
type NopLogger struct{}

func (NopLogger) Info(string, ...any)  {}
func (NopLogger) Warn(string, ...any)  {}
func (NopLogger) Error(string, ...any) {}
func (l NopLogger) With(...any) Logger { return l }
func (l NopLogger) WithContext(context.Context) Logger {
	return l
}
