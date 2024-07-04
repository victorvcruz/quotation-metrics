package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"quotation-metrics/internal/trade"
	"time"
)

type Quotation struct {
	service trade.Service
}

func (q *Quotation) GetMetrics(w http.ResponseWriter, r *http.Request) {
	ticker := r.URL.Query().Get("ticker")
	date := r.URL.Query().Get("date")

	if ticker == "" {
		http.Error(w, "Missing ticker", http.StatusBadRequest)
		return
	}

	var dateTime time.Time
	var err error
	if date != "" {
		dateTime, err = time.Parse("2006-01-02", date)
		if err != nil {
			http.Error(w, "Failed to parse date", http.StatusBadRequest)
			return
		}
	}

	metrics, err := q.service.Metrics(r.Context(), ticker, dateTime)
	if err != nil {
		http.Error(w, "Failed to get metrics", http.StatusInternalServerError)
		return
	}

	marshal, err := json.Marshal(metrics)
	if err != nil {
		http.Error(w, "Failed to marshal metrics", http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(marshal)
}

func (q *Quotation) BatchUpload(w http.ResponseWriter, r *http.Request) {
	file, _, err := r.FormFile("Quotation")
	if err != nil {
		http.Error(w, "Failed to get file", http.StatusBadRequest)
		return
	}

	// background process
	go func() {
		defer file.Close()
		err := q.service.BatchInsert(context.Background(), file)
		if err != nil {
			switch err {
			default:
				http.Error(w, "Failed to insert data", http.StatusInternalServerError)
			}
		}
	}()

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"message":"file uploaded successfully"}`))
}

func NewQuotation(service trade.Service) *Quotation {
	return &Quotation{
		service: service,
	}
}
