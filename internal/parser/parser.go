package parser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/condrove10/dukascopy-downloader/tick"
	"github.com/kjk/lzma"
	"io"
	"time"
)

const TickBytes = 20

func Decode(data []byte, symbol string, date time.Time) ([]*tick.Tick, error) {
	dec := lzma.NewReader(bytes.NewBuffer(data[:]))
	defer dec.Close()

	ticksArr := make([]*tick.Tick, 0)
	bytesArr := make([]byte, TickBytes)

	for {
		n, err := dec.Read(bytesArr[:])
		if err == io.EOF {
			err = nil
			break
		}
		if n != TickBytes || err != nil {
			return nil, fmt.Errorf("decode failed: %d: %v", n, err)
		}

		t, err := decodeTickData(bytesArr[:], symbol, date)
		if err != nil {
			return nil, fmt.Errorf("decode failed: %d: %v", n, err)
		}

		ticksArr = append(ticksArr, t)
	}

	return ticksArr, nil
}

func decodeTickData(data []byte, symbol string, timeH time.Time) (*tick.Tick, error) {
	raw := struct {
		TimeMs    int32
		Ask       int32
		Bid       int32
		VolumeAsk float32
		VolumeBid float32
	}{}

	if len(data) != TickBytes {
		return nil, errors.New("invalid length for tick data")
	}

	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.BigEndian, &raw); err != nil {
		return nil, err
	}

	var point float64 = 1000

	t := tick.Tick{
		Symbol:    symbol,
		Timestamp: timeH.UnixNano() + int64(raw.TimeMs)*int64(time.Millisecond),
		Ask:       float64(raw.Ask) / point,
		Bid:       float64(raw.Bid) / point,
		VolumeAsk: float64(raw.VolumeAsk),
		VolumeBid: float64(raw.VolumeBid),
	}

	return &t, nil
}
