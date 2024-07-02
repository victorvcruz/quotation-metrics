package config

import (
	"os"
	"strconv"
)

type Database struct {
	Host     string
	User     string
	Password string
	Port     string
	DbName   string
	SSLMode  string
	TimeZone string
}

type App struct {
	BatchSize int
	Workers   int
}

type Config struct {
	Database Database
	App      App
}

func Load() (*Config, error) {
	batchSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		return nil, err
	}

	workers, err := strconv.Atoi(os.Getenv("WORKERS"))
	if err != nil {
		return nil, err
	}

	return &Config{
		Database: Database{
			Host:     os.Getenv("POSTGRES_HOST"),
			User:     os.Getenv("POSTGRES_USER"),
			Password: os.Getenv("POSTGRES_PASSWORD"),
			Port:     os.Getenv("POSTGRES_PORT"),
			DbName:   os.Getenv("POSTGRES_DB"),
			SSLMode:  os.Getenv("POSTGRES_SLLMODE"),
			TimeZone: os.Getenv("POSTGRES_TIMEZONE"),
		},
		App: App{
			BatchSize: batchSize,
			Workers:   workers,
		},
	}, nil
}
