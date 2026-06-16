package binance

import "encoding/json"

// KlineEvent is the outer envelope Binance sends over the WebSocket.
type KlineEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Kline     Kline  `json:"k"`
}

// Kline is the candle data inside the "k" object.
//
// Prices/volumes use json.Number (not float64) to protect precision and to
// tolerate the feed sending either a JSON string ("66834.00") or a bare
// number. Convert to float only later, deliberately, in the feature pipeline.
type Kline struct {
	StartTime int64       `json:"t"`
	CloseTime int64       `json:"T"`
	Symbol    string      `json:"s"`
	Interval  string      `json:"i"`
	Open      json.Number `json:"o"`
	Close     json.Number `json:"c"`
	High      json.Number `json:"h"`
	Low       json.Number `json:"l"`
	Volume    json.Number `json:"v"`
	NumTrades int         `json:"n"`
	IsClosed  bool        `json:"x"` // true only when the minute closes
}