package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSlogLogger(t *testing.T) {
	t.Run("uses provided logger", func(t *testing.T) {
		var buf bytes.Buffer
		handler := slog.NewTextHandler(&buf, nil)
		l := NewSlogLogger(slog.New(handler))

		l.Info("hello", "k", "v")
		assert.Contains(t, buf.String(), "hello")
		assert.Contains(t, buf.String(), "k=v")
	})

	t.Run("falls back to default logger when nil", func(t *testing.T) {
		l := NewSlogLogger(nil)
		assert.NotNil(t, l)
		// Should not panic.
		l.Info("no-op check")
	})
}

func TestSlogLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	l := NewSlogLogger(slog.New(handler))

	l.Info("info msg")
	l.Warn("warn msg")
	l.Error("error msg")

	out := buf.String()
	assert.Contains(t, out, "info msg")
	assert.Contains(t, out, "warn msg")
	assert.Contains(t, out, "error msg")
	assert.Contains(t, out, "level=INFO")
	assert.Contains(t, out, "level=WARN")
	assert.Contains(t, out, "level=ERROR")
}

func TestSlogLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	base := NewSlogLogger(slog.New(handler))

	derived := base.With("component", "test")
	derived.Info("derived message")

	assert.Contains(t, buf.String(), "component=test")
	assert.Contains(t, buf.String(), "derived message")
}

func TestSlogLoggerWithContext(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	base := NewSlogLogger(slog.New(handler))

	derived := base.WithContext(context.Background())
	derived.Info("with context")

	assert.Contains(t, buf.String(), "with context")
	assert.Contains(t, buf.String(), "context=")
}

func TestNopLogger(t *testing.T) {
	var l Logger = NopLogger{}

	// None of these should panic.
	l.Info("info", "k", "v")
	l.Warn("warn")
	l.Error("error")

	assert.Equal(t, l, l.With("k", "v"))
	assert.Equal(t, l, l.WithContext(context.Background()))
}

func TestSlogLoggerImplementsLoggerInterface(t *testing.T) {
	var _ Logger = NewSlogLogger(nil)
	var _ Logger = NopLogger{}

	// Sanity check the interface methods chain without panicking.
	l := NewSlogLogger(nil).With("a", "b").WithContext(context.Background())
	assert.False(t, strings.Contains("", "unused"))
	l.Info("chained")
}
