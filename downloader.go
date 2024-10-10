package downloader

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/condrove10/dukascopy-downloader/internal/parser"
	"github.com/condrove10/dukascopy-downloader/internal/tick"
	"github.com/condrove10/dukascopy-downloader/internal/timeformat"
	"github.com/condrove10/dukascopy-downloader/pkg/conversions"
	"github.com/condrove10/dukascopy-downloader/pkg/csvencoder"
	"github.com/condrove10/dukascopy-downloader/pkg/retryablehttp"
	"github.com/go-playground/validator/v10"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

const urlTemplate = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5"

type Downloader struct {
	Symbol      string       `validate:"required,min=3"`
	StartTime   time.Time    `validate:"required"`
	EndTime     time.Time    `validate:"required"`
	Concurrency int          `validate:"required,gt=0"`
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

func (d *Downloader) WithConcurrency(concurrency int) *Downloader {
	d.Concurrency = concurrency
	return d
}

func (d *Downloader) WithHttpClient(httpClient *http.Client) *Downloader {
	d.HttpClient = httpClient
	return d
}

func (d *Downloader) Download() ([]*tick.Tick, error) {
	ticks := []*tick.Tick{}

	s, err := d.Stream(1)

	for {
		select {
		case t, open := <-s:
			if !open {
				sort.Slice(ticks, func(i, j int) bool {
					return ticks[i].Timestamp < ticks[j].Timestamp
				})
				return ticks, nil
			}

			ticks = append(ticks, t)
		case err, open := <-err:
			if !open {
				return ticks, nil
			}

			return nil, fmt.Errorf("error downloading %s: %v", d.Symbol, err)
		}
	}

}

func (d *Downloader) Stream(bufferSize int) (<-chan *tick.Tick, <-chan error) {
	streamChan := make(chan *tick.Tick, bufferSize)
	errorChan := make(chan error, 1)
	if err := validator.New().Struct(d); err != nil {
		errorChan <- fmt.Errorf("failed to validate downloader instance: %w", err)
		return streamChan, errorChan
	}

	if d.EndTime.Before(d.StartTime) {
		errorChan <- fmt.Errorf("end time must be after start time")
		return streamChan, errorChan
	}

	dates := timeformat.GetDateTimeRange(d.StartTime, d.EndTime, 1)
	concurrencyChan := make(chan struct{}, d.Concurrency)
	var wg sync.WaitGroup

	expected := 0

	runConcurrentTask(func() error {
		for _, date := range dates {
			wg.Add(1)

			runControlledTask(func() error {
				defer wg.Done()

				batch, err := d.fetchTicksForDate(date)
				if err != nil {
					return fmt.Errorf("failed to fetch ticks for date %s: %w", date, err)
				}

				expected += len(batch)
				for _, t := range batch {
					streamChan <- t
				}

				return nil
			}, concurrencyChan, errorChan)
		}

		wg.Wait()

		close(streamChan)

		return nil
	}, errorChan)

	return streamChan, errorChan
}

func (d *Downloader) ToCsv(filePath string) error {
	ce := csvencoder.NewCSVEncoder()
	ce.SetSeparator(';')

	ticksSlice, err := d.Download()
	if err != nil {
		return fmt.Errorf("failed to download ticks: %w", err)
	}

	ticksMapSpice := []map[string]interface{}{}
	for _, t := range ticksSlice {
		ticksMap, err := conversions.StructToMap(t, "csv")
		if err != nil {
			return fmt.Errorf("failed to convert ticks: %w", err)
		}

		ticksMapSpice = append(ticksMapSpice, ticksMap)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	defer f.Close()

	if err := ce.Encode(f, ticksMapSpice); err != nil {
		return fmt.Errorf("failed to encode csv: %w", err)
	}

	return nil
}

func runConcurrentTask(task func() error, errorChan chan error) {
	go func() {
		if err := task(); err != nil {
			errorChan <- err
		}

		close(errorChan)
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

	client := retryablehttp.DefaultClient.WithContext(context.Background()).WithUrl(url).WithHttpClient(d.HttpClient).WithMethod(retryablehttp.MethodGet).
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

func (d *Downloader) fetchTicksForDate(date time.Time) ([]*tick.Tick, error) {
	data, err := d.fetch(date.UTC())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}

	parsedTicks, err := parser.Decode(data, d.Symbol, date)
	if err != nil {
		return nil, fmt.Errorf("failed to parse data: %w", err)
	}

	if time.Date(d.StartTime.Year(), d.StartTime.Month(), d.StartTime.Day(), d.StartTime.Hour(), 0, 0, 0, d.StartTime.Location()).Equal(date) {
		tmp := make([]*tick.Tick, 0)
		for _, t := range parsedTicks {
			if time.Unix(0, t.Timestamp).After(d.StartTime) || time.Unix(0, t.Timestamp).Equal(d.StartTime) {
				tmp = append(tmp, t)
			}
		}

		return tmp, nil
	}

	if time.Date(d.EndTime.Year(), d.EndTime.Month(), d.EndTime.Day(), d.EndTime.Hour(), 0, 0, 0, d.EndTime.Location()).Equal(date) {
		tmp := make([]*tick.Tick, 0)
		for _, t := range parsedTicks {
			if time.Unix(0, t.Timestamp).Before(d.EndTime) || time.Unix(0, t.Timestamp).Equal(d.EndTime) {
				tmp = append(tmp, t)
			}
		}

		return tmp, nil
	}

	return parsedTicks, nil
}
