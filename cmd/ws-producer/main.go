package main

import (
	"encoding/json"
	"log"

	"binance-streaming/internal/binance"

	"github.com/gorilla/websocket"
)

// ws-producer connects to the Binance kline_1m WebSocket for one symbol,
// parses each event, and (currently) prints it. The Kafka producer is the
// next step to wire in — see internal/kafka/producer.go.
//
// NOTE on filtering: for kline_1m we only care about CLOSED candles
// (IsClosed == true). But that filter is KLINE-SPECIFIC. aggTrade/bookTicker
// have no "closed" concept, so don't make "drop unclosed" a universal
// producer rule, or you'll silently drop all trade/depth events later.
func main() {
	url := "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("dial failed:", err)
	}
	defer conn.Close()

	log.Println("connected, waiting for klines...")

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			return
		}

		var event binance.KlineEvent
		if err := json.Unmarshal(message, &event); err != nil {
			log.Println("parse error:", err)
			continue
		}

		// Only act on closed candles (real, completed data).
		if !event.Kline.IsClosed {
			continue
		}

		log.Printf("%s close=%s closed=%v",
			event.Symbol, event.Kline.Close.String(), event.Kline.IsClosed)

		// TODO (step 3): send raw `message` bytes to Kafka, keyed by symbol.
		// Store the RAW bytes (not the parsed struct) so Kafka stays the
		// source of truth and you can replay/re-feature later.
	}
}