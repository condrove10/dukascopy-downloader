package main

import (
	"github.com/condrove10/dukascopy-downloader/internal/downloader"
	"log"
	"net/http"
	"time"
)

func main() {
	now := time.Now().UTC()
	d := downloader.DefaultDownloader.WithSymbol("BTCUSD").WithConcurrency(5).WithStartTime(now.AddDate(0, 0,
		-1)).WithEndTime(now).WithHttpClient(http.DefaultClient)
	ticks, err := d.Download()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Took %v to download: %d ticks; with a %d datafeed requests concurrency", time.Now().UTC().Sub(now), len(ticks), d.Concurrency)
}
