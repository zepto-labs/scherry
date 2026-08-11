package scherry

// alias_logging.go covers internal/logging: the Logger interface and its
// slog-backed and no-op implementations.

import (
	"github.com/zepto-labs/scherry/internal/logging"
)

type (
	Logger    = logging.Logger
	nopLogger = logging.NopLogger
)

var NewSlogLogger = logging.NewSlogLogger
