package quotation

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"io"
	"log"
	"quotation-metrics/internal/config"
	"strconv"
	"strings"
	"time"
)

type Service interface {
	BatchInsert(ctx context.Context, reader io.Reader) error
	Metrics(ctx context.Context, ticker string, date time.Time) (*Metric, error)
}

type service struct {
	repository Repository
	cfg        config.Config
}

// Metrics returns the metrics for a given ticker and date
func (s *service) Metrics(ctx context.Context, ticker string, date time.Time) (*Metric, error) {
	start := time.Now()

	metrics, err := s.repository.GetMetrics(ctx, ticker, date)
	if err != nil {
		return nil, err
	}

	log.Println("end metrics found, elapsed time ", time.Since(start))

	return metrics, nil
}

// BatchInsert reads the csv file from the buffer and inserts the trades into the database
// It also calculates the metrics for the trades and inserts them into the database
func (s *service) BatchInsert(ctx context.Context, reader io.Reader) error {

	tradeCh := make(chan []*Trade)
	doneCh := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	// set workers to process the trades
	for i := 0; i < s.cfg.App.Workers; i++ {
		go s.worker(ctx, tradeCh, doneCh)
	}

	// process the csv file and send the trades to the workers
	metrics, err := s.processCSV(reader, tradeCh, ctx)
	if err != nil {
		return err
	}

	// close the trade channel
	// wait for the workers to finish
	close(tradeCh)
	for i := 0; i < s.cfg.App.Workers; i++ {
		if err = <-doneCh; err != nil {
			return err
		}
	}

	err = s.repository.BatchInsertMetrics(ctx, metrics)
	if err != nil {
		return err
	}

	log.Printf("end batch insert, total trade %d, elapsed time %s\n", len(metrics), time.Since(start))

	return nil
}

func (s *service) processCSV(reader io.Reader, tradeCh chan []*Trade, ctx context.Context) (map[string]*Metric, error) {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'

	var lineNum int
	var tradeList []*Trade
	metrics := make(map[string]*Metric)

	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Println("failed to read csv ", err)
			return nil, err
		}

		lineNum++

		if lineNum == 1 {
			continue
		}

		// parse the csv record to a trade
		trade, err := s.parseRecord(record)
		if err != nil {
			log.Println("failed to parse record ", err)
			return nil, err
		}

		tradeList = append(tradeList, trade)

		// add the trade to the metrics map
		s.updateMetrics(metrics, trade)

		// send the trades to the workers when the batch size is reached
		if len(tradeList) == s.cfg.App.BatchSize {
			select {
			case tradeCh <- tradeList:
				tradeList = nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// send the remaining trades to the workers
	if len(tradeList) > 0 {
		select {
		case tradeCh <- tradeList:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return metrics, nil
}

func (s *service) parseRecord(record []string) (*Trade, error) {
	tradePrice, err := decimal.NewFromString(strings.Replace(record[3], ",", ".", 1))
	if err != nil {
		return nil, fmt.Errorf("failed to parse trade price: %w", err)
	}

	tradeQuantity, err := strconv.Atoi(record[4])
	if err != nil {
		return nil, fmt.Errorf("failed to parse trade quantity: %w", err)
	}

	tradeDate, err := time.Parse("2006-01-02", record[8])
	if err != nil {
		return nil, fmt.Errorf("failed to parse trade date: %w", err)
	}

	return &Trade{
		InstrumentCode: record[1],
		TradePrice:     tradePrice,
		TradeQuantity:  tradeQuantity,
		CloseTime:      record[5],
		TradeDate:      tradeDate,
	}, nil
}

func (s *service) updateMetrics(metrics map[string]*Metric, trade *Trade) {
	if v, ok := metrics[trade.InstrumentCode]; ok {
		if trade.TradePrice.GreaterThan(v.MaxRangeValue) {
			v.MaxRangeValue = trade.TradePrice
		}
		v.MaxDailyVolume += trade.TradeQuantity
		metrics[trade.InstrumentCode] = v
	} else {
		metrics[trade.InstrumentCode] = &Metric{
			Ticker:         trade.InstrumentCode,
			MaxRangeValue:  trade.TradePrice,
			MaxDailyVolume: trade.TradeQuantity,
			TradeDate:      trade.TradeDate,
		}
	}
}

func (s *service) worker(ctx context.Context, tradeCh chan []*Trade, doneCh chan error) {
	for trades := range tradeCh {
		err := s.repository.BatchInsertTrade(ctx, trades)
		if err != nil {
			doneCh <- err
			return
		}
	}
	doneCh <- nil
}

func NewService(repository Repository) Service {
	return &service{
		repository: repository,
	}
}
