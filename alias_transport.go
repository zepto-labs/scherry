package scherry

// alias_transport.go covers internal/transport: the Kafka reader/writer
// wrappers and the task publisher built on top of them.

import (
	"github.com/zepto-labs/scherry/internal/transport"
)

type (
	MessageReader = transport.MessageReader
	MessageWriter = transport.MessageWriter
	RetryMessage  = transport.RetryMessage
)

var (
	NewKafkaReader   = transport.NewKafkaReader
	NewKafkaWriter   = transport.NewKafkaWriter
	NewTaskPublisher = transport.NewTaskPublisher
)
