package kafka

// Producer wraps a Kafka writer for the raw_klines topic.
//
// This is the step-3 component (not yet wired into ws-producer). Uses
// segmentio/kafka-go (pure Go, no C dependency).
//
// To enable:
//   go get github.com/segmentio/kafka-go
// then uncomment the implementation below.

/*
import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafkago.Writer
}

// NewProducer creates a writer pointed at the local broker and topic.
func NewProducer(broker, topic string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(broker), // e.g. "localhost:9092"
			Topic:    topic,               // e.g. "raw_klines"
			Balancer: &kafkago.Hash{},     // same key -> same partition
		},
	}
}

// Send writes one raw message keyed by symbol.
// key  = symbol (e.g. "BTCUSDT") -> controls partition + ordering
// value = the RAW JSON bytes from the WebSocket (source of truth)
func (p *Producer) Send(ctx context.Context, symbol string, raw []byte) error {
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(symbol),
		Value: raw,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
*/