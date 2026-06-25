package main

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const baseURL = "https://data.binance.vision/data/spot/monthly/klines"

var symbols = []string{
	"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
	"ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
	"POLUSDT", "LTCUSDT", "TRXUSDT", "ATOMUSDT", "UNIUSDT", // POLUSDT not MATICUSDT (rebrand)
	"NEARUSDT", "APTUSDT", "FILUSDT", "ETCUSDT", "XLMUSDT",
}

var months = []string{
	"2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10",
	"2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04",
}

func main() {
	for _, symbol := range symbols {
		for _, month := range months {
			if err := downloadMonth(symbol, month); err != nil {
				// One failure (404 for a thin/new symbol, network blip)
				// shouldn't kill the whole 240-file run.
				log.Printf("FAILED %s %s: %v", symbol, month, err)
				continue
			}
		}
	}
	log.Println("done")
}

func downloadMonth(symbol, month string) error {
	fileName := fmt.Sprintf("%s-1m-%s", symbol, month)
	zipURL := fmt.Sprintf("%s/%s/1m/%s.zip", baseURL, symbol, fileName)
	checksumURL := zipURL + ".CHECKSUM"

	outDir := filepath.Join("data", "klines", symbol)
	csvPath := filepath.Join(outDir, fileName+".csv")

	// Idempotent: skip files we already have, so re-runs are cheap.
	if _, err := os.Stat(csvPath); err == nil {
		log.Printf("skip (exists) %s", csvPath)
		return nil
	}

	log.Printf("downloading %s", fileName)

	zipBytes, err := fetch(zipURL)
	if err != nil {
		return fmt.Errorf("download zip: %w", err)
	}

	checksumBytes, err := fetch(checksumURL)
	if err != nil {
		return fmt.Errorf("download checksum: %w", err)
	}
	if err := verifyChecksum(zipBytes, checksumBytes); err != nil {
		return err
	}

	if err := unzipToCSV(zipBytes, outDir); err != nil {
		return fmt.Errorf("unzip: %w", err)
	}
	return nil
}

// fetch downloads a URL fully into memory (needed so we can hash the bytes).
func fetch(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d for %s", resp.StatusCode, url)
	}
	return io.ReadAll(resp.Body)
}

// verifyChecksum compares SHA-256 of the zip to the published checksum.
func verifyChecksum(zipBytes, checksumBytes []byte) error {
	// CHECKSUM file format: "<hash>  <filename>"
	expected := strings.Fields(string(checksumBytes))[0]

	sum := sha256.Sum256(zipBytes)
	got := hex.EncodeToString(sum[:])

	if got != expected {
		return fmt.Errorf("checksum mismatch: got %s, want %s", got, expected)
	}
	return nil
}

// unzipToCSV extracts the CSV(s) from the zip into outDir.
func unzipToCSV(zipBytes []byte, outDir string) error {
	reader, err := zip.NewReader(strings.NewReader(string(zipBytes)), int64(len(zipBytes)))
	if err != nil {
		return err
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	for _, f := range reader.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}

		outPath := filepath.Join(outDir, f.Name)
		outFile, err := os.Create(outPath)
		if err != nil {
			rc.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		rc.Close()
		outFile.Close()
		if err != nil {
			return err
		}
		log.Printf("saved %s", outPath)
	}
	return nil
}