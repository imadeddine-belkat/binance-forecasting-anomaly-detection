package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// gap-filler covers the period the monthly archive hasn't published yet
// (last archived month -> today) using the Binance REST API.
//
// REST returns up to 1000 klines per request, so we PAGINATE: fetch 1000,
// advance startTime past the last row, repeat until we reach now.
//
// IMPORTANT: REST timestamps are in MILLISECONDS, while the archive files are
// MICROSECONDS. The Spark batch job normalizes both to ms at load time.

const restURL = "https://api.binance.com/api/v3/klines"

var symbols = []string{
	"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
	"ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
	"POLUSDT", "LTCUSDT", "TRXUSDT", "ATOMUSDT", "UNIUSDT",
	"NEARUSDT", "APTUSDT", "FILUSDT", "ETCUSDT", "XLMUSDT",
}

func main() {
	// Gap start: first ms of the first month NOT covered by the archive.
	// Archive ran through April 2026 -> gap starts May 1, 2026.
	// (Adjust if your archive coverage differs.)
	gapStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	gapEnd := time.Now().UTC().UnixMilli()

	for _, symbol := range symbols {
		if err := fillGap(symbol, gapStart, gapEnd); err != nil {
			log.Printf("FAILED %s: %v", symbol, err)
			continue
		}
	}
	log.Println("done")
}

func fillGap(symbol string, start, end int64) error {
	var allRows [][]string

	for start < end {
		klines, err := fetchKlines(symbol, start, end)
		if err != nil {
			return err
		}
		if len(klines) == 0 {
			break
		}

		for _, k := range klines {
			allRows = append(allRows, klineToCSVRow(k))
		}

		// Pagination engine: advance to 1 minute past the last open time.
		// Get this wrong and you either skip data or loop forever.
		lastOpenTime := int64(klines[len(klines)-1][0].(float64))
		start = lastOpenTime + 60_000

		if len(klines) < 1000 {
			break // fewer than a full page => caught up to now
		}

		time.Sleep(200 * time.Millisecond) // respect REST rate limits
	}

	if len(allRows) == 0 {
		log.Printf("no gap data for %s", symbol)
		return nil
	}
	return writeCSV(symbol, allRows)
}

// fetchKlines returns up to 1000 raw klines as mixed-type arrays.
func fetchKlines(symbol string, start, end int64) ([][]interface{}, error) {
	url := fmt.Sprintf("%s?symbol=%s&interval=1m&startTime=%d&endTime=%d&limit=1000",
		restURL, symbol, start, end)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var klines [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&klines); err != nil {
		return nil, err
	}
	return klines, nil
}

// klineToCSVRow flattens one REST kline array into CSV strings,
// matching the Binance Vision archive's 12-field format.
// REST arrays mix numbers (timestamps) and strings (prices), hence the switch.
func klineToCSVRow(k []interface{}) []string {
	row := make([]string, len(k))
	for i, field := range k {
		switch v := field.(type) {
		case float64:
			row[i] = strconv.FormatInt(int64(v), 10)
		case string:
			row[i] = v
		default:
			row[i] = fmt.Sprintf("%v", v)
		}
	}
	return row
}

func writeCSV(symbol string, rows [][]string) error {
	outDir := filepath.Join("data", "klines", symbol)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	path := filepath.Join(outDir, symbol+"-1m-gap.csv")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.WriteAll(rows); err != nil {
		return err
	}
	log.Printf("wrote %d rows to %s", len(rows), path)
	return nil
}