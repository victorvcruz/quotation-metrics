package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"quotation-metrics/internal/trade"
	"testing"
	"time"
)

type mockService struct {
	mock.Mock
}

func (m *mockService) BatchInsert(ctx context.Context, reader io.Reader) error {
	args := m.Called(ctx, reader)
	return args.Error(0)
}

func (m *mockService) Metrics(ctx context.Context, ticker string, date time.Time) (*trade.Metric, error) {
	args := m.Called(ctx, ticker, date)
	return args.Get(0).(*trade.Metric), args.Error(1)
}

func TestGetMetrics(t *testing.T) {
	cases := []struct {
		name string
		req  struct {
			ticker string
			date   string
		}
		mockFunc func(m *mockService)
		err      error
		status   int
		want     string
	}{
		{
			name: "success",
			req: struct {
				ticker string
				date   string
			}{ticker: "GOOG", date: "2024-06-20"},
			mockFunc: func(m *mockService) {
				m.On("Metrics", mock.Anything, "GOOG", time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC)).
					Return(&trade.Metric{
						Ticker:         "GOOG",
						MaxDailyVolume: 11,
						MaxRangeValue:  decimal.NewFromInt(29),
					}, nil).Once()
			},
			status: http.StatusOK,
			want:   "{\"ticker\":\"GOOG\",\"max_range_value\":\"29\",\"max_daily_volume\":11}",
		},
		{
			name: "failed because error in metrics",
			req: struct {
				ticker string
				date   string
			}{ticker: "GOOG", date: "2024-06-20"},
			mockFunc: func(m *mockService) {
				m.On("Metrics", mock.Anything, "GOOG", time.Date(2024, 06, 20, 0, 0, 0, 0, time.UTC)).
					Return((*trade.Metric)(nil), errors.New("mock-error")).Once()
			},
			status: http.StatusInternalServerError,
			want:   "Failed to get metrics\n",
		},
		{
			name: "failed because error parse date",
			req: struct {
				ticker string
				date   string
			}{ticker: "GOOG", date: "2024-06-2J"},
			mockFunc: func(m *mockService) {},
			status:   http.StatusBadRequest,
			want:     "Failed to parse date\n",
		},
		{
			name: "failed because error missing ticker",
			req: struct {
				ticker string
				date   string
			}{ticker: "", date: ""},
			mockFunc: func(m *mockService) {},
			status:   http.StatusBadRequest,
			want:     "Missing ticker\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := new(mockService)
			tc.mockFunc(m)

			s := NewQuotation(m)

			req, err := http.NewRequest("GET", fmt.Sprintf("/metrics?ticker=%s&date=%s", tc.req.ticker, tc.req.date), nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			rr := httptest.NewRecorder()

			r := chi.NewRouter()
			r.Get("/metrics", s.GetMetrics)

			r.ServeHTTP(rr, req)

			assert.Equal(t, rr.Code, tc.status)
			assert.Equal(t, rr.Body.String(), tc.want)
		})
	}
}

func TestBatchUpload(t *testing.T) {
	cases := []struct {
		name     string
		req      string
		mockFunc func(m *mockService)
		status   int
		want     string
	}{
		{
			name: "success",
			req:  "header\nGOOG,29,11,2024-06-20\n",
			mockFunc: func(m *mockService) {
				m.On("BatchInsert", mock.Anything, mock.Anything).
					Return(nil)
			},
			status: http.StatusOK,
			want:   `{"message":"file uploaded successfully"}`,
		},
		{
			name:     "missing file",
			req:      "",
			mockFunc: func(m *mockService) {},
			status:   http.StatusBadRequest,
			want:     "Failed to get file\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := new(mockService)
			if tc.mockFunc != nil {
				tc.mockFunc(m)
			}

			q := &Quotation{service: m}

			body := &bytes.Buffer{}
			header := http.Header{}

			if tc.req != "" {
				writer := multipart.NewWriter(body)
				part, err := writer.CreateFormFile("Quotation", "example.csv")
				if err != nil {
					t.Fatalf("failed to create form file: %v", err)
				}
				_, err = part.Write([]byte(tc.req))
				if err != nil {
					t.Fatalf("failed to write to form file: %v", err)
				}
				err = writer.Close()
				if err != nil {
					t.Fatalf("failed to close multipart writer: %v", err)
				}

				header.Set("Content-Type", writer.FormDataContentType())
			}

			req, err := http.NewRequest("POST", "/upload", body)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header = header

			rr := httptest.NewRecorder()

			r := chi.NewRouter()
			r.Post("/upload", q.BatchUpload)

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.status, rr.Code)
			assert.Equal(t, tc.want, rr.Body.String())
		})
	}
}
