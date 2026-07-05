package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	Logger Logger
	Server Server `envPrefix:"SERVER_"`
	KeyDB  DB     `envPrefix:"KEY_DB_"`
	DataDB DB     `envPrefix:"DATA_DB_"`
}

type DB struct {
	Addr     string `env:"ADDR"`
	User     string `env:"USER"`
	Pass     string `env:"PASS"`
	Name     string `env:"NAME"`
	Host     string `env:"HOST"`
	Port     string `env:"PORT"`
	SSLMode  string `env:"SSL_MODE" envDefault:"disable"`
	MaxConns int    `env:"MAX_CONN"`
	MinConns int    `env:"MIN_CONN"`
	DSN      string `env:"DSN"`

	// пагинация для data repository
	PageSize int `env:"PAGE_SIZE" envDefault:"1000"`
}

type Server struct {
	Addr string `env:"ADDR"`
}

type Logger struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"INFO"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

func (d DB) ConnectionString() string {
	if d.DSN != "" {
		return d.DSN
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		d.User, d.Pass, d.Host, d.Port, d.Name, d.SSLMode,
	)
}
