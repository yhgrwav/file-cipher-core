package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	Logger   Logger
	Server   Server   `envPrefix:"SERVER_"`
	KeyDB    DB       `envPrefix:"KEY_DB_"`
	DataDB   DB       `envPrefix:"DATA_DB_"`
	Redis    Redis    `envPrefix:"REDIS_"`
	Flusher  Flusher  `envPrefix:"FLUSHER_"`
	Cipher   Cipher   `envPrefix:"CIPHER_"`
	Rotation Rotation `envPrefix:"ROTATION_"`
	Decipher Decipher `envPrefix:"DECIPHER_"`
	Pending  Pending  `envPrefix:"PENDING_"`
	Retry    Retry    `envPrefix:"RETRY_"`
}

// Retry - общие для всех сервисов настройки повторов при ошибках БД/Redis
type Retry struct {
	Attempts int           `env:"ATTEMPTS" envDefault:"3"`
	Backoff  time.Duration `env:"BACKOFF" envDefault:"100ms"`
}

type Cipher struct {
	ChunkSize int `env:"CHUNK_SIZE" envDefault:"51200"`
	Workers   int `env:"WORKERS" envDefault:"8"`
}

type Redis struct {
	Addr       string        `env:"ADDR" envDefault:"localhost:6379"`
	Password   string        `env:"PASSWORD"`
	DB         int           `env:"DB" envDefault:"0"`
	CursorTTL  time.Duration `env:"CURSOR_TTL" envDefault:"24h"`
	Queue      string        `env:"QUEUE" envDefault:"cipher:pending"`
	CursorName string        `env:"CURSOR_NAME" envDefault:"cool_cursor"`
	ReadBlock  time.Duration `env:"READ_BLOCK" envDefault:"5s"`
}

type Flusher struct {
	BatchSize            int           `env:"BATCH_SIZE" envDefault:"1000"`
	FlushTime            time.Duration `env:"FLUSH_TIME" envDefault:"200ms"`
	ShutdownFlushTimeout time.Duration `env:"SHUTDOWN_FLUSH_TIMEOUT" envDefault:"5s"`
}

type Rotation struct {
	PageSize int `env:"PAGE_SIZE" envDefault:"1000"`
	Workers  int `env:"WORKERS" envDefault:"8"`
}

type Decipher struct {
	PageSize int `env:"PAGE_SIZE" envDefault:"1000"`
}

type Pending struct {
	// Amount - сколько записей за раз вычитывать из Redis (XReadGroup Count / XAutoClaim Count)
	Amount int `env:"AMOUNT" envDefault:"1000"`

	// ClaimInterval - как часто пытаться забрать зависшие записи из PEL
	ClaimInterval time.Duration `env:"CLAIM_INTERVAL" envDefault:"30s"`

	// ClaimMinIdle - сколько запись должна провисеть в PEL без Ack, чтобы считаться зависшей
	ClaimMinIdle time.Duration `env:"CLAIM_MIN_IDLE" envDefault:"1m"`

	// ReadBackoff - стартовая пауза после неудачного чтения из Redis
	ReadBackoff time.Duration `env:"READ_BACKOFF" envDefault:"100ms"`

	// ReadBackoffMax - потолок, до которого растёт эта пауза
	ReadBackoffMax time.Duration `env:"READ_BACKOFF_MAX" envDefault:"5s"`
}

type DB struct {
	User     string `env:"USER"`
	Pass     string `env:"PASS"`
	Name     string `env:"NAME"`
	Host     string `env:"HOST"`
	Port     string `env:"PORT"`
	SSLMode  string `env:"SSL_MODE" envDefault:"disable"`
	MaxConns int    `env:"MAX_CONN"`
	MinConns int    `env:"MIN_CONN"`
	DSN      string `env:"DSN"`
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
