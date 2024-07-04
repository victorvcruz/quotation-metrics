package trade

import (
	"bytes"
	"context"
	"errors"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/big"
	"quotation-metrics/internal/config"
	"testing"
	"time"
)

type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) BatchInsertTrade(ctx context.Context, trades []*Trade) error {
	args := m.Called(ctx, trades)
	return args.Error(0)
}

func (m *MockRepository) GetMetrics(ctx context.Context, ticker string, date time.Time) (*Metric, error) {
	args := m.Called(ctx, ticker, date)
	if args.Get(0) != nil {
		return args.Get(0).(*Metric), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockRepository) BatchInsertMetrics(ctx context.Context, metricsMap map[string]*Metric) error {
	args := m.Called(ctx, metricsMap)
	return args.Error(0)
}

func TestServiceMetrics(t *testing.T) {
	cases := []struct {
		name     string
		ticker   string
		date     time.Time
		mockFunc func(m *MockRepository)
		want     *Metric
		wantErr  error
	}{
		{
			name:   "success",
			ticker: "AAPL",
			date:   time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
			mockFunc: func(m *MockRepository) {
				m.On("GetMetrics", mock.Anything, "AAPL", time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC)).
					Return(&Metric{
						Ticker:         "AAPL",
						MaxRangeValue:  decimal.NewFromInt(100),
						MaxDailyVolume: 50,
					}, nil).Once()
			},
			want: &Metric{
				Ticker:         "AAPL",
				MaxRangeValue:  decimal.NewFromInt(100),
				MaxDailyVolume: 50,
			},
			wantErr: nil,
		},
		{
			name:   "failed because repository error",
			ticker: "AAPL",
			date:   time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC),
			mockFunc: func(m *MockRepository) {
				m.On("GetMetrics", mock.Anything, "AAPL", time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC)).
					Return(nil, errors.New("repository error")).Once()
			},
			want:    nil,
			wantErr: errors.New("repository error"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockRepo := new(MockRepository)
			tc.mockFunc(mockRepo)

			cfg := &config.Config{}
			svc := NewService(mockRepo, cfg)

			got, err := svc.Metrics(context.Background(), tc.ticker, tc.date)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.want, got)
			mockRepo.AssertExpectations(t)
		})
	}
}

func TestService_BatchInsert(t *testing.T) {
	testCases := []struct {
		name       string
		csvContent string
		mockFunc   func(m *MockRepository)
		wantErr    error
	}{
		{
			name: "success",
			csvContent: `DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
2024-06-28;TF583R;0;10,000;10000;041646257;10;1;2024-06-28;100;100
2024-06-28;DI1F25;0;10,600;6;090000017;10;1;2024-06-28;3;23
2024-06-28;DI1F25;0;10,601;9;090000017;20;1;2024-06-28;3;23
2024-06-28;DI1N24;0;10,398;1;090000017;10;1;2024-06-28;114;114
`,
			mockFunc: func(m *MockRepository) {
				m.On("BatchInsertTrade", mock.Anything, []*Trade{
					{
						InstrumentCode: "TF583R",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10000), -3),
						TradeQuantity:  10000,
						CloseTime:      "041646257",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					{
						InstrumentCode: "DI1F25",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10600), -3),
						TradeQuantity:  6,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(nil).Once()

				m.On("BatchInsertTrade", mock.Anything, []*Trade{
					{
						InstrumentCode: "DI1F25",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10601), -3),
						TradeQuantity:  9,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					{
						InstrumentCode: "DI1N24",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10398), -3),
						TradeQuantity:  1,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(nil).Once()

				m.On("BatchInsertMetrics", mock.Anything, map[string]*Metric{
					"TF583R": {
						Ticker:         "TF583R",
						MaxRangeValue:  decimal.NewFromBigInt(big.NewInt(10000), -3),
						MaxDailyVolume: 10000,
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					"DI1F25": {
						Ticker:         "DI1F25",
						MaxRangeValue:  decimal.NewFromBigInt(big.NewInt(10601), -3),
						MaxDailyVolume: 15,
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					"DI1N24": {
						Ticker:         "DI1N24",
						MaxRangeValue:  decimal.NewFromBigInt(big.NewInt(10398), -3),
						MaxDailyVolume: 1,
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(nil).Once()
			},
			wantErr: nil,
		},
		{
			name: "failed because error in batch insert metrics",
			csvContent: `DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
2024-06-28;TF583R;0;10,000;10000;041646257;10;1;2024-06-28;100;100
2024-06-28;DI1F25;0;10,600;6;090000017;10;1;2024-06-28;3;23
2024-06-28;DI1F25;0;10,600;9;090000017;20;1;2024-06-28;3;23
2024-06-28;DI1N24;0;10,398;1;090000017;10;1;2024-06-28;114;114
`,
			mockFunc: func(m *MockRepository) {
				m.On("BatchInsertTrade", mock.Anything, []*Trade{
					{
						InstrumentCode: "TF583R",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10000), -3),
						TradeQuantity:  10000,
						CloseTime:      "041646257",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					{
						InstrumentCode: "DI1F25",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10600), -3),
						TradeQuantity:  6,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(nil).Once()

				m.On("BatchInsertTrade", mock.Anything, []*Trade{
					{
						InstrumentCode: "DI1F25",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10600), -3),
						TradeQuantity:  9,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					{
						InstrumentCode: "DI1N24",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10398), -3),
						TradeQuantity:  1,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(nil).Once()

				m.On("BatchInsertMetrics", mock.Anything, map[string]*Metric{
					"TF583R": {
						Ticker:         "TF583R",
						MaxRangeValue:  decimal.NewFromBigInt(big.NewInt(10000), -3),
						MaxDailyVolume: 10000,
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					"DI1F25": {
						Ticker:         "DI1F25",
						MaxRangeValue:  decimal.NewFromBigInt(big.NewInt(10600), -3),
						MaxDailyVolume: 15,
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					"DI1N24": {
						Ticker:         "DI1N24",
						MaxRangeValue:  decimal.NewFromBigInt(big.NewInt(10398), -3),
						MaxDailyVolume: 1,
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(errors.New("mock-error")).Once()
			},
			wantErr: errors.New("mock-error"),
		},
		{
			name: "failed because error in batch insert trade",
			csvContent: `DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
2024-06-28;TF583R;0;10,000;10000;041646257;10;1;2024-06-28;100;100
2024-06-28;DI1F25;0;10,600;6;090000017;10;1;2024-06-28;3;23
2024-06-28;DI1F25;0;10,600;9;090000017;20;1;2024-06-28;3;23
2024-06-28;DI1N24;0;10,398;1;090000017;10;1;2024-06-28;114;114
`,
			mockFunc: func(m *MockRepository) {
				m.On("BatchInsertTrade", mock.Anything, []*Trade{
					{
						InstrumentCode: "TF583R",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10000), -3),
						TradeQuantity:  10000,
						CloseTime:      "041646257",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
					{
						InstrumentCode: "DI1F25",
						TradePrice:     decimal.NewFromBigInt(big.NewInt(10600), -3),
						TradeQuantity:  6,
						CloseTime:      "090000017",
						TradeDate:      time.Date(2024, 06, 28, 0, 0, 0, 0, time.UTC),
					},
				}).Return(errors.New("mock-error")).Once()
			},
			wantErr: errors.New("context canceled"),
		},
		{
			name: "failed because error in parse trade date",
			csvContent: `DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
2024-06-28;TF583R;0;10,000;10000;041646257;10;1;2024-0-28;100;100
2024-06-28;DI1F25;0;10,600;6;090000017;10;1;2024-06-28;3;23
2024-06-28;DI1F25;0;10,600;9;090000017;20;1;2024-06-28;3;23
2024-06-28;DI1N24;0;10,398;1;090000017;10;1;2024-06-28;114;114
`,
			mockFunc: func(m *MockRepository) {},
			wantErr:  errors.New("failed to parse trade date: parsing time \"2024-0-28\" as \"2006-01-02\": cannot parse \"0-28\" as \"01\""),
		},
		{
			name: "failed because error in parse trade quantity",
			csvContent: `DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
2024-06-28;TF583R;0;i,000;10000;041646257;10;1;2024-0-28;100;100
2024-06-28;DI1F25;0;10,600;6;090000017;10;1;2024-06-28;3;23
2024-06-28;DI1F25;0;10,600;9;090000017;20;1;2024-06-28;3;23
2024-06-28;DI1N24;0;10,398;1;090000017;10;1;2024-06-28;114;114
`,
			mockFunc: func(m *MockRepository) {},
			wantErr:  errors.New("failed to parse trade price: can't convert i.000 to decimal"),
		},
		{
			name: "failed because error in parse trade price",
			csvContent: `DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
2024-06-28;TF583R;0;1j,000;10000;041646257;10;1;2024-06-28;100;100
2024-06-28;DI1F25;0;10,600;6;090000017;10;1;2024-06-28;3;23
2024-06-28;DI1F25;0;10,600;9;090000017;20;1;2024-06-28;3;23
2024-06-28;DI1N24;0;10,398;1;090000017;10;1;2024-06-28;114;114
`,
			mockFunc: func(m *MockRepository) {},
			wantErr:  errors.New("failed to parse trade price: can't convert 1j.000 to decimal"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				App: config.App{
					Workers:   1,
					BatchSize: 2,
				},
			}

			mockRepo := new(MockRepository)

			tc.mockFunc(mockRepo)

			svc := NewService(mockRepo, cfg)

			reader := bytes.NewReader([]byte(tc.csvContent))
			err := svc.BatchInsert(context.Background(), reader)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
