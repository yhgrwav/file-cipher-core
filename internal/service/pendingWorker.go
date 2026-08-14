package service

import (
	"context"
	"fmt"
	"time"

	"file-cipher-core/internal/entity"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type (
	KeysRepo interface {
		SaveKeys(ctx context.Context, keys []entity.ChunkKey) error
	}
	DataRepo interface {
		SaveData(ctx context.Context, data []entity.ChunkData) error
	}
	PendingStore interface {
		Read(ctx context.Context, amount int) ([]entity.PendingItem, error)
		Ack(ctx context.Context, ids []string) error
		Claim(ctx context.Context, minIdle time.Duration, amount int) ([]entity.PendingItem, error)
	}
)

type PendingWorkerConfig struct {
	// Amount - сколько записей за раз вычитывать из Redis
	Amount int

	// ClaimInterval - как часто пытаться забрать зависшие записи из PEL
	ClaimInterval time.Duration

	// ClaimMinIdle - сколько запись должна провисеть в PEL без Ack, чтобы считаться зависшей
	ClaimMinIdle time.Duration

	// WriteRetries - сколько раз повторить запись батча в Postgres при ошибке
	WriteRetries int

	// WriteRetryBackoff - базовая пауза между повторами записи в Postgres
	WriteRetryBackoff time.Duration
}

// PendingWorker дописывает в Postgres то, что Flusher уже надёжно сложил в Redis: читает батчами из
// consumer group (Read), пишет в keys/data репозитории и подтверждает (Ack). Claim параллельно подбирает
// записи, зависшие в PEL после падения инстанса (своего прошлого или чужого).
type PendingWorker struct {
	pdb    PendingStore
	data   DataRepo
	keys   KeysRepo
	logger *zap.Logger
	cfg    PendingWorkerConfig
}

func NewPendingWorker(pdb PendingStore, data DataRepo, keys KeysRepo, logger *zap.Logger, cfg PendingWorkerConfig) *PendingWorker {
	return &PendingWorker{
		pdb:    pdb,
		data:   data,
		keys:   keys,
		logger: logger,
		cfg:    cfg,
	}
}

// Run запускает чтение новых записей и добор зависших параллельно, до отмены ctx.
func (p *PendingWorker) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return p.readLoop(ctx)
	})
	g.Go(func() error {
		return p.claimLoop(ctx)
	})

	return g.Wait()
}

// readLoop вычитывает новые записи. Read блокируется на стороне Redis (readBlock), поэтому отдельный
// тикер здесь не нужен - пустой ответ просто означает, что Redis прождал впустую и вернул управление.
func (p *PendingWorker) readLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		items, err := p.pdb.Read(ctx, p.cfg.Amount)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.logger.Error("pending read failed", zap.Error(err))
			continue
		}
		if len(items) == 0 {
			continue
		}

		if err := p.process(ctx, items); err != nil {
			p.logger.Error("pending process failed", zap.Int("count", len(items)), zap.Error(err))
		}
	}
}

// claimLoop периодически забирает записи, зависшие в PEL дольше ClaimMinIdle - то, что кто-то (в том
// числе этот же инстанс в прошлой жизни) прочитал через Read, но так и не заакал.
func (p *PendingWorker) claimLoop(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.ClaimInterval)
	defer ticker.Stop()

	for {
		items, err := p.pdb.Claim(ctx, p.cfg.ClaimMinIdle, p.cfg.Amount)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.logger.Error("pending claim failed", zap.Error(err))
		} else if len(items) > 0 {
			if err := p.process(ctx, items); err != nil {
				p.logger.Error("pending claim process failed", zap.Int("count", len(items)), zap.Error(err))
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// process пишет батч в Postgres (keys и data параллельно) и, только при полном успехе, подтверждает
// записи в Redis. Частичная запись (например, keys прошли, а data - нет) не подтверждается вовсе,
// чтобы запись осталась в PEL и была подобрана повторно, а не потерялась и не задвоилась в Postgres.
func (p *PendingWorker) process(ctx context.Context, items []entity.PendingItem) error {
	ids := make([]string, 0, len(items))
	data := make([]entity.ChunkData, 0, len(items))
	keys := make([]entity.ChunkKey, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
		data = append(data, item.Item.Data)
		keys = append(keys, item.Item.Key)
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return withRetry(gCtx, p.cfg.WriteRetries, p.cfg.WriteRetryBackoff, func() error {
			return p.keys.SaveKeys(gCtx, keys)
		})
	})
	g.Go(func() error {
		return withRetry(gCtx, p.cfg.WriteRetries, p.cfg.WriteRetryBackoff, func() error {
			return p.data.SaveData(gCtx, data)
		})
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("write pending batch (%d): %w", len(items), err)
	}

	if err := p.pdb.Ack(ctx, ids); err != nil {
		return fmt.Errorf("ack pending batch (%d): %w", len(items), err)
	}
	p.logger.Debug("pending batch flushed to postgres", zap.Int("count", len(items)))
	return nil
}