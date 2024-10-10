package main

import (
	dukascopy_downloader "github.com/condrove10/dukascopy-downloader"
	"log"
	"net/http"
	"time"
)

func main() {
	now := time.Now()
	d := dukascopy_downloader.DefaultDownloader.WithSymbol("BTCUSD").WithConcurrency(5).WithStartTime(now.Add(time.Hour * -6)).WithEndTime(now).WithHttpClient(
		http.
			DefaultClient)
	err := d.ToCsv("./ticks.csv")
	if err != nil {
		log.Fatal(err)
	}
}
