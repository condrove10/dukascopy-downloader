package cursor

import (
	"context"
	"errors"
	"github.com/condrove10/dukascopy-downloader/tick"
)

var (
	ErrNoCurrentData = errors.New("no current data to read")
	ErrCursorClosed  = errors.New("cursor is closed")
)

// Cursor manages data and error channels, mimicking a cursor's behavior.
type Cursor struct {
	dataCh       <-chan *tick.Tick
	errCh        <-chan error
	current      *tick.Tick
	closed       bool
	error        error
	bufferLength uint64
}

// NewCursor initializes a new Cursor with data and error channels.
func NewCursor(dataCh <-chan *tick.Tick, errCh <-chan error) *Cursor {
	return &Cursor{
		dataCh: dataCh,
		errCh:  errCh,
	}
}

// Next advances the cursor to the next data point.
// It returns true if there is a next data point, and false if the cursor is exhausted or an error occurred.
func (c *Cursor) Next(ctx context.Context) bool {
	if c.closed {
		if c.bufferLength > 0 {
			c.current = <-c.dataCh
			c.bufferLength--
			return true
		}

		return false
	}

	select {
	case <-ctx.Done():
		c.error = ctx.Err()
		c.closed = true
		return false

	case err, ok := <-c.errCh:
		if !ok {
			c.bufferLength = uint64(len(c.dataCh))
			c.closed = true
			return true
		}

		if err != nil {
			c.error = err
			c.closed = true
			return false
		}
		return true

	case data, ok := <-c.dataCh:
		if !ok {
			c.bufferLength = uint64(len(c.dataCh))
			c.closed = true
			return true
		}
		c.current = data
		return true
	}
}

// Read returns the current data point.
func (c *Cursor) Read() *tick.Tick {
	return c.current
}

func (c *Cursor) Error() error {
	return c.error
}
