package tick

type Tick struct {
	Symbol    string  `validate:"required" json:"symbol"`
	Timestamp int64   `validate:"required" json:"timestamp"`
	Ask       float64 `validate:"required" json:"ask"`
	Bid       float64 `validate:"required" json:"bid"`
	VolumeAsk float64 `validate:"required" json:"volume_ask"`
	VolumeBid float64 `validate:"required" json:"volume_bid"`
}

func New() *Tick {
	return &Tick{}
}

func (t *Tick) WithSymbol(symbol string) *Tick {
	t.Symbol = symbol
	return t
}

func (t *Tick) WithTimestamp(timestamp int64) *Tick {
	t.Timestamp = timestamp
	return t
}

func (t *Tick) WithAsk(ask float64) *Tick {
	t.Ask = ask
	return t
}

func (t *Tick) WithBid(bid float64) *Tick {
	t.Bid = bid
	return t
}

func (t *Tick) WithVolumeAsk(ask float64) *Tick {
	t.VolumeAsk = ask
	return t
}

func (t *Tick) WithVolumeBid(bid float64) *Tick {
	t.VolumeBid = bid
	return t
}
