package trade

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"
	"time"
)

func TestBatchInsertTrade(t *testing.T) {
	cases := []struct {
		name     string
		trades   []*Trade
		mockFunc func(sqlmock.Sqlmock)
		wantErr  error
	}{
		{
			name: "success",
			trades: []*Trade{
				{
					InstrumentCode: "GOOG",
					TradePrice:     decimal.NewFromFloat(1500.25),
					TradeQuantity:  10,
					CloseTime:      "15:00:00",
					TradeDate:      time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
				},
				{
					InstrumentCode: "AAPL",
					TradePrice:     decimal.NewFromFloat(1300.50),
					TradeQuantity:  15,
					CloseTime:      "15:00:00",
					TradeDate:      time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
				},
			},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO trades (instrument_code, trade_price, trade_quantity, close_time, trade_date) VALUES ($1, $2, $3, $4, $5),($6, $7, $8, $9, $10)`)).
					WithArgs("GOOG", decimal.NewFromFloat(1500.25), 10, "15:00:00", time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
						"AAPL", decimal.NewFromFloat(1300.50), 15, "15:00:00", time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnResult(sqlmock.NewResult(2, 2))
				mock.ExpectCommit()
			},
			wantErr: nil,
		},
		{
			name: "failed because insert error",
			trades: []*Trade{
				{
					InstrumentCode: "GOOG",
					TradePrice:     decimal.NewFromFloat(1500.25),
					TradeQuantity:  10,
					CloseTime:      "15:00:00",
					TradeDate:      time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
				},
			},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO trades (instrument_code, trade_price, trade_quantity, close_time, trade_date) VALUES ($1, $2, $3, $4, $5)`)).
					WithArgs("GOOG", decimal.NewFromFloat(1500.25), 10, "15:00:00", time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnError(errors.New("insert error"))
				mock.ExpectRollback()
			},
			wantErr: errors.New("insert error"),
		},
		{
			name: "failed because commit error",
			trades: []*Trade{
				{
					InstrumentCode: "GOOG",
					TradePrice:     decimal.NewFromFloat(1500.25),
					TradeQuantity:  10,
					CloseTime:      "15:00:00",
					TradeDate:      time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
				},
			},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO trades (instrument_code, trade_price, trade_quantity, close_time, trade_date) VALUES ($1, $2, $3, $4, $5)`)).
					WithArgs("GOOG", decimal.NewFromFloat(1500.25), 10, "15:00:00", time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit().WillReturnError(errors.New("commit error"))
			},
			wantErr: errors.New("commit error"),
		},
		{
			name: "failed because begin error",
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin error"))
			},
			wantErr: errors.New("begin error"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			assert.NoError(t, err)
			defer db.Close()

			tc.mockFunc(mock)

			r := NewRepository(db)

			err = r.BatchInsertTrade(context.Background(), tc.trades)
			assert.Equal(t, tc.wantErr, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBatchInsertMetrics(t *testing.T) {
	cases := []struct {
		name     string
		metrics  map[string]*Metric
		mockFunc func(sqlmock.Sqlmock)
		wantErr  error
	}{
		{
			name: "success",
			metrics: map[string]*Metric{
				"GOOG": {
					Ticker:         "GOOG",
					MaxRangeValue:  decimal.NewFromInt(29),
					MaxDailyVolume: 11,
					TradeDate:      time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC),
				},
			},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO metrics (ticker, max_range_value, max_daily_volume, trade_date) VALUES ($1, $2, $3, $4)`)).
					WithArgs("GOOG", decimal.NewFromInt(29), 11, time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnResult(sqlmock.NewResult(2, 2))
				mock.ExpectCommit()
			},
			wantErr: nil,
		},
		{
			name: "failed because insert error",
			metrics: map[string]*Metric{
				"GOOG": {
					Ticker:         "GOOG",
					MaxRangeValue:  decimal.NewFromInt(29),
					MaxDailyVolume: 11,
					TradeDate:      time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC),
				},
			},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO metrics (ticker, max_range_value, max_daily_volume, trade_date) VALUES ($1, $2, $3, $4)`)).
					WithArgs("GOOG", decimal.NewFromInt(29), 11, time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnError(errors.New("insert error"))
				mock.ExpectRollback()
			},
			wantErr: errors.New("insert error"),
		},
		{
			name: "failed because commit error",
			metrics: map[string]*Metric{
				"GOOG": {
					Ticker:         "GOOG",
					MaxRangeValue:  decimal.NewFromInt(29),
					MaxDailyVolume: 11,
					TradeDate:      time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC),
				},
			},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO metrics (ticker, max_range_value, max_daily_volume, trade_date) VALUES ($1, $2, $3, $4)`)).
					WithArgs("GOOG", decimal.NewFromInt(29), 11, time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit().WillReturnError(errors.New("commit error"))
			},
			wantErr: errors.New("commit error"),
		},
		{
			name: "failed because begin error",
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin error"))
			},
			wantErr: errors.New("begin error"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tc.mockFunc(mock)

			r := NewRepository(db)

			err = r.BatchInsertMetrics(context.Background(), tc.metrics)
			assert.Equal(t, tc.wantErr, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
func TestGetMetrics(t *testing.T) {
	cases := []struct {
		name     string
		ticker   string
		date     time.Time
		mockFunc func(sqlmock.Sqlmock)
		want     *Metric
		wantErr  error
	}{
		{
			name:   "success with date",
			ticker: "GOOG",
			date:   time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC),
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT m.ticker, MAX(m.max_range_value), MAX(m.max_daily_volume) FROM metrics m WHERE m.ticker = $1 AND m.trade_date >= $2 GROUP BY m.ticker;`)).
					WithArgs("GOOG", time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC)).
					WillReturnRows(sqlmock.NewRows([]string{"ticker", "max_range_value", "max_daily_volume"}).
						AddRow("GOOG", 29, 11))
			},
			want: &Metric{
				Ticker:         "GOOG",
				MaxRangeValue:  decimal.NewFromInt(29),
				MaxDailyVolume: 11,
			},
			wantErr: nil,
		},
		{
			name:   "success without date",
			ticker: "AAPL",
			date:   time.Time{},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT m.ticker, MAX(m.max_range_value), MAX(m.max_daily_volume) FROM metrics m WHERE m.ticker = $1 GROUP BY m.ticker;`)).
					WithArgs("AAPL").
					WillReturnRows(sqlmock.NewRows([]string{"ticker", "max_range_value", "max_daily_volume"}).
						AddRow("AAPL", 50, 20))
			},
			want: &Metric{
				Ticker:         "AAPL",
				MaxRangeValue:  decimal.NewFromInt(50),
				MaxDailyVolume: 20,
			},
		},
		{
			name:   "failed because query error",
			ticker: "MSFT",
			date:   time.Time{},
			mockFunc: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT m.ticker, MAX(m.max_range_value), MAX(m.max_daily_volume) FROM metrics m WHERE m.ticker = $1 GROUP BY m.ticker;`)).
					WithArgs("MSFT").
					WillReturnError(errors.New("query error"))
			},
			want:    nil,
			wantErr: errors.New("query error"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			assert.NoError(t, err)
			defer db.Close()

			tc.mockFunc(mock)

			r := NewRepository(db)

			got, err := r.GetMetrics(context.Background(), tc.ticker, tc.date)

			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantErr, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
