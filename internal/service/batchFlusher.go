package service

import (
	"context"
	"file-cipher-core/internal/repository"
	"fmt"
	"time"

	"file-cipher-core/internal/entity"

	"go.uber.org/zap"
)


// SUMMARY:
// получаем данные -> пишем их в redis (на диск, не в память) -> отдаём список айдишников в pendingWorker ->
// там данные дописываются из redis в postgres

type pendingWriter interface {
	AddBatch(ctx context.Context, items []entity.FlushItem) ([]string, error)
}

type FlusherConfig struct {
	// BatchSize - при скольких накопленных парах батч пишется в БД принудительно
	BatchSize int

	// FlushTime - максимальное время, которое пара ждёт в батче при слабом потоке данных
	FlushTime time.Duration

	// ShutdownFlushTimeout - таймаут на дозапись данных при отмене контекста
	ShutdownFlushTimeout time.Duration

	// WriteRetries - сколько раз повторить запись батча при ошибке БД
	WriteRetries int

	// WriteRetryBackoff - базовая пауза между повторами
	WriteRetryBackoff time.Duration
}

// Flusher решает проблему записи данных в базу, базовый паттерн, задаётся BatchSize и FlushTime -> если батч переполнился -
// принудительно опустошается, отгружая все данные в БД, если поток данных маленький, но всё же есть - срабатывает FlushTime,
// который по определённому кулдауну вызывает flush. в зависимости передаётся оба репозитория и логгер, ничего необычного
type Flusher struct {
	pendingRepo pendingWriter
	logger      *zap.Logger
	cfg         FlusherConfig
}

func NewFlusher(pendingRepo *repository.PendingStore, logger *zap.Logger, cfg FlusherConfig) *Flusher {
	return &Flusher{
		pendingRepo: pendingRepo,
		logger:      logger,
		cfg:         cfg,
	}
}

// Run читает результаты из in до закрытия канала, батчами записывая их в Redis.
// Возвращает ошибку первой неудачной записи; при штатном завершении (in закрыт)
// дописывает остаток и возвращает nil
func (f *Flusher) Run(ctx context.Context, in <-chan entity.FlushItem) error {
	items := make([]entity.FlushItem, 0, f.cfg.BatchSize)

	timer := time.NewTimer(f.cfg.FlushTime)
	defer timer.Stop()

	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		if err := f.write(ctx, items); err != nil {
			return err
		}
		items = items[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			if len(items) > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), f.cfg.ShutdownFlushTimeout)
				err := f.write(flushCtx, items)
				cancel()
				if err != nil {
					f.logger.Error("flush tail on shutdown failed", zap.Int("lost", len(items)), zap.Error(err))
				}
			}
			f.logger.Info("Flusher worker done")
			return ctx.Err()

		case v, ok := <-in:
			if !ok {
				return flush()
			}
			items = append(items, v)
			if len(items) >= f.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
				resetTimer(timer, f.cfg.FlushTime)
			}

		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
			timer.Reset(f.cfg.FlushTime)
		}
	}
}

// write кладёт пачку в Redis
func (f *Flusher) write(ctx context.Context, items []entity.FlushItem) error {
	if err := withRetry(ctx, f.cfg.WriteRetries, f.cfg.WriteRetryBackoff, func() error {
		_, err := f.pendingRepo.AddBatch(ctx, items)
		return err
	}); err != nil {
		return fmt.Errorf("flush pending batch (%d): %w", len(items), err)
	}
	f.logger.Debug("batch flushed to pending", zap.Int("count", len(items)))
	return nil
}

// хелпер для ресета таймера
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
