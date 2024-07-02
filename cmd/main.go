package main

import (
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"quotation-metrics/cmd/handlers"
	"quotation-metrics/internal/config"
	"quotation-metrics/internal/platform"
	"quotation-metrics/internal/quotation"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("failed to load environment variables %s\n", err.Error())
	}
}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration %v", err)
	}

	db, err := platform.PostgresConnect(cfg)
	if err != nil {
		log.Fatalf("failed to connect to postgres %v", err)
	}

	err = platform.RunMigrations(db)
	if err != nil {
		log.Fatalf("failed to run migrations %v", err)
	}

	quotationRepository := quotation.NewRepository(db)

	quotationService := quotation.NewService(quotationRepository, cfg)

	quotationHandler := handlers.NewQuotation(quotationService)

	r.Post("/upload", quotationHandler.BatchUpload)
	r.Get("/metrics", quotationHandler.GetMetrics)

	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalln("failed to start server on port 8080")
	}
	log.Println("server started on port 8080")
}
