package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"file-cipher-core/internal/config"
	v1 "file-cipher-core/internal/handler/http/v1"
	"file-cipher-core/internal/repository"
	"file-cipher-core/internal/service"
	lg "file-cipher-core/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger, loggerClose, err := lg.NewLogger(cfg.Logger.LogLevel)
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer func() {
		_ = loggerClose()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	keyPool, err := newPool(ctx, cfg.KeyDB)
	if err != nil {
		logger.Fatal("connect key db", "error", err)
	}
	defer keyPool.Close()
	keyRepo := repository.NewKeyRepository(keyPool)

	dataPool, err := newPool(ctx, cfg.DataDB)
	if err != nil {
		logger.Fatal("connect data db", "error", err)
	}
	defer dataPool.Close()
	dataRepo := repository.NewDataRepository(dataPool)

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal("connect redis", "error", err)
	}
	defer func() {
		_ = rdb.Close()
	}()
	cursors := repository.NewCursorStore(rdb, cfg.Redis.CursorTTL)

	pendingCfg := repository.PendingStoreConfig{
		Queue:      cfg.Redis.Queue,
		CursorName: cfg.Redis.CursorName,
		ReadBlock:  cfg.Redis.ReadBlock,
	}

	pendingStore, err := repository.NewPendingStore(rdb, logger, pendingCfg)
	if err != nil {
		logger.Fatal("init pending store", "error", err)
	}
	if err := pendingStore.CursorInit(ctx); err != nil {
		logger.Fatal("init pending cursor", "error", err)
	}

	logger.Info("dependencies connected")

	retryCfg := service.RetryConfig{
		Attempts: cfg.Retry.Attempts,
		Backoff:  cfg.Retry.Backoff,
	}

	flusher := service.NewFlusher(pendingStore, logger, service.FlusherConfig{
		BatchSize:            cfg.Flusher.BatchSize,
		FlushTime:            cfg.Flusher.FlushTime,
		ShutdownFlushTimeout: cfg.Flusher.ShutdownFlushTimeout,
		Retry:                retryCfg,
	})

	pendingWorker := service.NewPendingWorker(pendingStore, dataRepo, keyRepo, logger, service.PendingWorkerConfig{
		Amount:         cfg.Pending.Amount,
		ClaimInterval:  cfg.Pending.ClaimInterval,
		ClaimMinIdle:   cfg.Pending.ClaimMinIdle,
		Retry:          retryCfg,
		ReadBackoff:    cfg.Pending.ReadBackoff,
		ReadBackoffMax: cfg.Pending.ReadBackoffMax,
	})
	go func() {
		if err := pendingWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("pending worker stopped", "error", err)
		}
	}()

	cipher := service.NewCipher(flusher, logger, service.CipherConfig{
		ChunkSize: cfg.Cipher.ChunkSize,
		Workers:   cfg.Cipher.Workers,
	})

	rotator := service.NewRotator(dataRepo, keyRepo, flusher, cursors, logger, service.RotatorConfig{
		PageSize: cfg.Rotation.PageSize,
		Workers:  cfg.Rotation.Workers,
		Retry:    retryCfg,
	})

	decipher := service.NewDecipher(service.DecipherConfig{
		PageSize: cfg.Decipher.PageSize,
		Retry:    retryCfg,
	}, logger, dataRepo, keyRepo)

	handler := v1.NewCipherHandler(cipher, rotator, decipher, logger)

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go serve(srv, "api", logger, errCh)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("server failed", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown", "error", err)
	}

	logger.Info("stopped gracefully")
}

func newPool(ctx context.Context, dbCfg config.DB) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dbCfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	if dbCfg.MaxConns > 0 {
		poolCfg.MaxConns = int32(dbCfg.MaxConns)
	}
	if dbCfg.MinConns > 0 {
		poolCfg.MinConns = int32(dbCfg.MinConns)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

func serve(srv *http.Server, name string, logger lg.Logger, errCh chan<- error) {
	logger.Info("http server listening", "service", name, "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errCh <- fmt.Errorf("%s server: %w", name, err)
	}
}
