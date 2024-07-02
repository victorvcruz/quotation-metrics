package quotation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Repository interface {
	BatchInsertTrade(ctx context.Context, trades []*Trade) error
	GetMetrics(ctx context.Context, ticker string, date time.Time) (*Metric, error)
	BatchInsertMetrics(ctx context.Context, metricsMap map[string]*Metric) error
}

type repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) Repository {
	return &repository{
		db: db,
	}
}

func (r *repository) BatchInsertTrade(ctx context.Context, trades []*Trade) error {
	valueStrings := make([]string, len(trades))
	valueArgs := make([]interface{}, 0, len(trades)*5)

	for i, trade := range trades {
		valueStrings[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5)
		valueArgs = append(valueArgs, trade.InstrumentCode, trade.TradePrice, trade.TradeQuantity, trade.CloseTime, trade.TradeDate)
	}
	stmt := fmt.Sprintf("INSERT INTO trade (instrument_code, trade_price, trade_quantity, close_time, trade_date) VALUES %s",
		strings.Join(valueStrings, ","))
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (r *repository) BatchInsertMetrics(ctx context.Context, metricsMap map[string]*Metric) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	valueStrings := make([]string, 0, len(metricsMap))
	valueArgs := make([]interface{}, 0, len(metricsMap)*4)
	argCounter := 1

	for ticker, metrics := range metricsMap {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d)", argCounter, argCounter+1, argCounter+2, argCounter+3))
		valueArgs = append(valueArgs, ticker, metrics.MaxRangeValue, metrics.MaxDailyVolume, metrics.TradeDate)
		argCounter += 4
	}

	stmt := fmt.Sprintf("INSERT INTO metrics (ticker, max_range_value, max_daily_volume, trade_date) VALUES %s", strings.Join(valueStrings, ","))
	_, err = tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (r *repository) GetMetrics(ctx context.Context, ticker string, date time.Time) (*Metric, error) {
	query := `
		SELECT 
			m.ticker,
			MAX(m.max_range_value),
			MAX(m.max_daily_volume)
		FROM 
			metrics m
		WHERE 
			m.ticker = $1 AND
			m.trade_date >= $2
		GROUP BY 
			m.ticker;
	`

	var data Metric
	err := r.db.QueryRowContext(ctx, query, ticker, date).Scan(
		&data.Ticker, &data.MaxRangeValue, &data.MaxDailyVolume,
	)
	if err != nil {
		return nil, err
	}

	return &data, nil
}
