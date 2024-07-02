package config

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {

	cases := []struct {
		name     string
		mockFunc func()
		err      error
		want     *Config
	}{
		{
			name: "success",
			mockFunc: func() {

				t.Setenv("POSTGRES_HOST", "localhost")
				t.Setenv("POSTGRES_USER", "testuser")
				t.Setenv("POSTGRES_PASSWORD", "testpassword")
				t.Setenv("POSTGRES_PORT", "5432")
				t.Setenv("POSTGRES_DB", "testdb")
				t.Setenv("POSTGRES_SLLMODE", "disable")
				t.Setenv("POSTGRES_TIMEZONE", "UTC")

				t.Setenv("BATCH_SIZE", "100")
				t.Setenv("WORKERS", "4")
			},
			want: &Config{
				Database: Database{
					Host:     "localhost",
					User:     "testuser",
					Password: "testpassword",
					Port:     "5432",
					DbName:   "testdb",
					SSLMode:  "disable",
					TimeZone: "UTC",
				},
				App: App{
					BatchSize: 100,
					Workers:   4,
				},
			},
		},
		{
			name: "failed because error in parse workers",
			mockFunc: func() {

				t.Setenv("POSTGRES_HOST", "localhost")
				t.Setenv("POSTGRES_USER", "testuser")
				t.Setenv("POSTGRES_PASSWORD", "testpassword")
				t.Setenv("POSTGRES_PORT", "5432")
				t.Setenv("POSTGRES_DB", "testdb")
				t.Setenv("POSTGRES_SLLMODE", "disable")
				t.Setenv("POSTGRES_TIMEZONE", "UTC")

				t.Setenv("BATCH_SIZE", "100")
				t.Setenv("WORKERS", "i")
			},
			want: nil,
			err: &strconv.NumError{
				Func: "Atoi",
				Num:  "i",
				Err:  errors.New("invalid syntax"),
			},
		},
		{
			name: "failed because error in parse batch size",
			mockFunc: func() {

				t.Setenv("POSTGRES_HOST", "localhost")
				t.Setenv("POSTGRES_USER", "testuser")
				t.Setenv("POSTGRES_PASSWORD", "testpassword")
				t.Setenv("POSTGRES_PORT", "5432")
				t.Setenv("POSTGRES_DB", "testdb")
				t.Setenv("POSTGRES_SLLMODE", "disable")
				t.Setenv("POSTGRES_TIMEZONE", "UTC")

				t.Setenv("BATCH_SIZE", "i")
			},
			want: nil,
			err: &strconv.NumError{
				Func: "Atoi",
				Num:  "i",
				Err:  errors.New("invalid syntax"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockFunc()
			cfg, err := Load()

			assert.Equal(t, err, tc.err)
			assert.EqualExportedValuesf(t, cfg, tc.want, "")
		})
	}
}
