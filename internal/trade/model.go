package trade

import (
	"github.com/shopspring/decimal"
	"time"
)

type Trade struct {
	ID             int             `json:"-"`
	InstrumentCode string          `json:"instrument_code"`
	TradePrice     decimal.Decimal `json:"trade_price"`
	TradeQuantity  int             `json:"trade_quantity"`
	CloseTime      string          `json:"close_time"`
	TradeDate      time.Time       `json:"trade_date"`
}

type Metric struct {
	ID             int             `json:"-"`
	Ticker         string          `json:"ticker"`
	MaxRangeValue  decimal.Decimal `json:"max_range_value"`
	MaxDailyVolume int             `json:"max_daily_volume"`
	TradeDate      time.Time       `json:"-"`
}
