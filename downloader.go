package dukascopy_downloader

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/condrove10/dukascopy-downloader/internal/parser"
	"github.com/condrove10/dukascopy-downloader/internal/tick"
	"github.com/condrove10/dukascopy-downloader/internal/timeformat"
	"github.com/condrove10/dukascopy-downloader/pkg/retryablehttp"
	"github.com/go-playground/validator/v10"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

const urlTemplate = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5"

type Downloader struct {
	Symbol      string       `validate:"required,min=3"`
	StartTime   time.Time    `validate:"required"`
	EndTime     time.Time    `validate:"required"`
	Concurrency uint16       `validate:"required,gt=0"`
	HttpClient  *http.Client `validate:"required"`
}

var DefaultDownloader = &Downloader{
	Concurrency: 1,
	HttpClient:  http.DefaultClient,
}

func (d *Downloader) WithSymbol(symbol string) *Downloader {
	d.Symbol = symbol
	return d
}

func (d *Downloader) WithStartTime(startTime time.Time) *Downloader {
	d.StartTime = startTime
	return d
}

func (d *Downloader) WithEndTime(endTime time.Time) *Downloader {
	d.EndTime = endTime
	return d
}

func (d *Downloader) WithConcurrency(concurrency uint16) *Downloader {
	d.Concurrency = concurrency
	return d
}

func (d *Downloader) WithHttpClient(httpClient *http.Client) *Downloader {
	d.HttpClient = httpClient
	return d
}

func (d *Downloader) Download() ([]*tick.Tick, error) {
	if err := validator.New().Struct(d); err != nil {
		return nil, fmt.Errorf("failed to validate downloader instance: %w", err)
	}

	dates := timeformat.GetDateTimeRange(d.StartTime, d.EndTime, 1)
	errorChan := make(chan error, 1)
	concurrencyChan := make(chan struct{}, d.Concurrency)
	ticks := []*tick.Tick{}
	var wg sync.WaitGroup

	runConcurrentTask(func() error {
		for _, date := range dates {
			wg.Add(1)

			runControlledTask(func() error {
				defer wg.Done()

				data, err := d.fetch(date)
				if err != nil {
					return fmt.Errorf("failed to fetch data: %w", err)
				}

				parsedTicks, err := parser.Decode(data, d.Symbol, date)
				if err != nil {
					return fmt.Errorf("failed to parse data: %w", err)
				}

				ticks = append(ticks, parsedTicks...)

				return nil
			}, concurrencyChan, errorChan)
		}

		wg.Wait()

		return nil
	}, errorChan)

	if err := <-errorChan; err != nil {
		return nil, fmt.Errorf("error downloading ticks: %s", err.Error())
	}

	sort.Slice(ticks, func(i, j int) bool {
		return ticks[i].Timestamp < ticks[j].Timestamp
	})

	return ticks, nil
}

func runConcurrentTask(task func() error, errorChan chan error) {
	go func() {
		errorChan <- task()
	}()
}

func runControlledTask(task func() error, concurrencyChan chan struct{}, errorChan chan error) {
	concurrencyChan <- struct{}{}

	go func() {
		if err := task(); err != nil {
			errorChan <- err
		}
		<-concurrencyChan
	}()
}

func (d *Downloader) fetch(date time.Time) ([]byte, error) {
	headers := map[string]string{
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
		"Accept":          "/",
		"Accept-Encoding": "gzip, deflate",
		"Origin":          "https://freeserv.dukascopy.com",
		"Connection":      "keep-alive",
		"Referer":         "https://freeserv.dukascopy.com/",
		"Cache-Control":   "no-cache",
	}

	url := fmt.Sprintf(urlTemplate, d.Symbol, date.Year(), date.Month()-1, date.Day(), date.Hour())

	client := retryablehttp.DefaultClient.WithContext(context.Background()).WithUrl(url).WithHttpClient(d.HttpClient).WithMethod(http.MethodGet).
		WithMaxRetries(5).WithRetryDelay(time.Second * 15).WithHeader(headers).WithRetryCondition(func(resp *http.Response, err error) bool {
		if err != nil || resp.StatusCode != http.StatusOK {
			return true
		}

		return false
	})

	resp, err := client.Do()
	if err != nil {
		return nil, fmt.Errorf("error fetching data for url '%s': %w", url, err)
	}
	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading data for url '%s': %w", url, err)
	}

	return content, nil
}
