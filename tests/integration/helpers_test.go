//go:build integration

// Интеграционные тесты гоняются против реального Redis/Postgres, поднятых через docker-compose
// (key-db на 5433, data-db на 5434, redis на 6379 - именно так они проброшены наружу в docker-compose.yml).
// Запуск: docker compose up -d key-db data-db redis migrate-keys migrate-data
// go test -tags=integration ./tests/integration/... -v
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	cfgpkg "file-cipher-core/internal/config"
	"file-cipher-core/internal/repository"
	"file-cipher-core/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: envOr("TEST_REDIS_ADDR", "localhost:6379"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s (start docker-compose first): %v", rdb.Options().Addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func newTestKeyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return newTestPool(t, "TEST_KEY_DB_DSN", "postgres://cipher:cipher@localhost:5433/keys?sslmode=disable")
}

func newTestDataPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return newTestPool(t, "TEST_DATA_DB_DSN", "postgres://cipher:cipher@localhost:5434/data?sslmode=disable")
}

func newTestPool(t *testing.T, envKey, def string) *pgxpool.Pool {
	t.Helper()
	dsn := envOr(envKey, def)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unavailable at %s (start docker-compose first): %v", dsn, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres unavailable at %s (start docker-compose first): %v", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// newTestPendingStore создаёт PendingStore с уникальным stream/group на тест, чтобы тесты не
// пересекались данными друг с другом и с предыдущими прогонами.
func newTestPendingStore(t *testing.T, rdb *redis.Client) *repository.PendingStore {
	t.Helper()
	suffix := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())

	store, err := repository.NewPendingStore(rdb, logger.NewNop(), cfgpkg.Redis{
		Queue:      "test:pending:" + suffix,
		CursorName: "test:group:" + suffix,
		ReadBlock:  200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new pending store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := store.CursorInit(ctx); err != nil {
		t.Fatalf("cursor init: %v", err)
	}
	return store
}
