package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"binance-streaming/internal/binance"
	"binance-streaming/internal/kafka"

	"github.com/gorilla/websocket"
)

var symbols = []string{
	"btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
	"adausdt", "dogeusdt", "avaxusdt", "dotusdt", "linkusdt",
	"ltcusdt", "trxusdt", "atomusdt", "etcusdt", "filusdt",
	"nearusdt", "uniusdt", "xlmusdt", "aptusdt", "polusdt",
}

type rawMsg struct {
	key   string
	value []byte
}

// Combined stream wraps each event: {"stream":"btcusdt@kline_1s","data":{...}}
type streamEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func buildURL() string {
	parts := make([]string, len(symbols))
	for i, s := range symbols {
		parts[i] = s + "@kline_1s"
	}
	return "wss://stream.binance.com:9443/stream?streams=" + strings.Join(parts, "/")
}

func main() {
	prod := kafka.NewProducer("localhost:9092", "raw_klines")
	defer prod.Close()
	ctx := context.Background()

	events := make(chan rawMsg, 2000) // buffer decouples socket read from Kafka send

	// N worker goroutines: drain channel -> Kafka. Concurrent sends; ordering
	// per symbol is preserved because each symbol always uses the same key.
	for i := 0; i < 4; i++ {
		go func() {
			for m := range events {
				if err := prod.Send(ctx, m.key, m.value); err != nil {
					log.Println("kafka send error:", err)
				}
			}
		}()
	}

	// reconnect loop: if the socket drops, redial
	for {
		if err := runConnection(events); err != nil {
			log.Println("connection error, reconnecting in 3s:", err)
			time.Sleep(3 * time.Second)
		}
	}
}

func runConnection(events chan rawMsg) error {
	conn, _, err := websocket.DefaultDialer.Dial(buildURL(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("connected, streaming %d symbols...", len(symbols))

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return err // bubble up -> reconnect
		}

		var env streamEnvelope
		if err := json.Unmarshal(message, &env); err != nil {
			log.Println("envelope parse error:", err)
			continue
		}

		var event binance.KlineEvent
		if err := json.Unmarshal(env.Data, &event); err != nil {
			log.Println("kline parse error:", err)
			continue
		}
		if !event.Kline.IsClosed {
			continue
		}

		// forward RAW inner data bytes, keyed by symbol
		events <- rawMsg{key: event.Symbol, value: env.Data}
	}
}