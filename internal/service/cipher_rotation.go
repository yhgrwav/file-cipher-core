package service

import (
	"context"
	"fmt"
	"time"

	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type (
	rotationDataReader interface {
		GetChunkUUIDsByFileID(ctx context.Context, fileID, afterUUID uuid.UUID, limit int) ([]uuid.UUID, error)
		GetLatestData(ctx context.Context, ids []uuid.UUID) ([]entity.ChunkData, error)
		DeleteOldData(ctx context.Context, ids []uuid.UUID) error
	}
	rotationKeyReader interface {
		GetLatestKeys(ctx context.Context, ids []uuid.UUID) ([]entity.ChunkKey, error)
		DeleteOldKeys(ctx context.Context, ids []uuid.UUID) error
	}
	cursorStore interface {
		Save(ctx context.Context, op string, cursor uuid.UUID) error
		Load(ctx context.Context, op string) (uuid.UUID, bool, error)
		Delete(ctx context.Context, op string) error
	}
)

type RotationJob struct {
	Current entity.ChunkData
	OldKey  entity.ChunkKey
}

type RotatorConfig struct {
	PageSize int
	Workers  int
	Retry    RetryConfig
}

type Rotator struct {
	data    rotationDataReader
	keys    rotationKeyReader
	flusher *Flusher
	cursors cursorStore
	logger  *zap.Logger
	cfg     RotatorConfig
}

func NewRotator(data rotationDataReader, keys rotationKeyReader, flusher *Flusher, cursors cursorStore, logger *zap.Logger, cfg RotatorConfig) *Rotator {
	return &Rotator{
		data:    data,
		keys:    keys,
		flusher: flusher,
		cursors: cursors,
		logger:  logger,
		cfg:     cfg,
	}
}

func (r *Rotator) Run(ctx context.Context, fileID uuid.UUID) error {
	r.logger.Info("rotation started", zap.String("file_id", fileID.String()))

	g, ctx := errgroup.WithContext(ctx)
	jobs := make(chan RotationJob, r.cfg.Workers)
	items := make(chan FlushItem, r.cfg.Workers)

	g.Go(func() error {
		return r.flusher.Run(ctx, items)
	})

	g.Go(func() error {
		wg, wctx := errgroup.WithContext(ctx)
		for i := 0; i < r.cfg.Workers; i++ {
			wg.Go(func() error {
				return rotationWorker(wctx, jobs, items)
			})
		}
		err := wg.Wait()
		close(items)
		return err
	})

	g.Go(func() error {
		defer close(jobs)
		return r.produce(ctx, fileID, jobs)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if err := r.deleteOldVersions(ctx, fileID); err != nil {
		return err
	}

	if err := r.cursors.Delete(ctx, fileID.String()); err != nil {
		r.logger.Warn("delete cursor failed", zap.String("file_id", fileID.String()), zap.Error(err))
	}

	r.logger.Info("rotation finished", zap.String("file_id", fileID.String()))
	return nil
}

// deleteOldVersions после успешной ротации постранично удаляет из обеих БД все версии чанков файла,
// кроме самой свежей. Список uuid берётся из БД данных, т.к. БД ключей file_id не хранит.
func (r *Rotator) deleteOldVersions(ctx context.Context, fileID uuid.UUID) error {
	cursor := uuid.Nil
	for {
		ids, err := r.data.GetChunkUUIDsByFileID(ctx, fileID, cursor, r.cfg.PageSize)
		if err != nil {
			return fmt.Errorf("get chunk uuids: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}

		if err := r.data.DeleteOldData(ctx, ids); err != nil {
			return fmt.Errorf("delete old data: %w", err)
		}
		if err := r.keys.DeleteOldKeys(ctx, ids); err != nil {
			return fmt.Errorf("delete old keys: %w", err)
		}

		cursor = ids[len(ids)-1]
	}
}

func (r *Rotator) produce(ctx context.Context, fileID uuid.UUID, out chan<- RotationJob) error {
	op := fileID.String()

	cursor, ok, err := r.cursors.Load(ctx, op)
	if err != nil {
		return fmt.Errorf("load cursor: %w", err)
	}
	if !ok {
		cursor = uuid.Nil
	}

	for {
		var ids []uuid.UUID
		if err := retryDo(ctx, r.cfg.Retry, func() error {
			var err error
			ids, err = r.data.GetChunkUUIDsByFileID(ctx, fileID, cursor, r.cfg.PageSize)
			return err
		}); err != nil {
			return fmt.Errorf("get chunk uuids: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}

		var chunks []entity.ChunkData
		if err := retryDo(ctx, r.cfg.Retry, func() error {
			var err error
			chunks, err = r.data.GetLatestData(ctx, ids)
			return err
		}); err != nil {
			return fmt.Errorf("get latest data: %w", err)
		}

		var keys []entity.ChunkKey
		if err := retryDo(ctx, r.cfg.Retry, func() error {
			var err error
			keys, err = r.keys.GetLatestKeys(ctx, ids)
			return err
		}); err != nil {
			return fmt.Errorf("get latest keys: %w", err)
		}

		keyByUUID := make(map[uuid.UUID]entity.ChunkKey, len(keys))
		for _, k := range keys {
			keyByUUID[k.UUID] = k
		}

		for _, chunk := range chunks {
			oldKey, ok := keyByUUID[chunk.UUID]
			if !ok {
				return fmt.Errorf("key not found for chunk %s: desync", chunk.UUID)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- RotationJob{Current: chunk, OldKey: oldKey}:
			}
		}

		cursor = ids[len(ids)-1]
		if err := r.cursors.Save(ctx, op, cursor); err != nil {
			r.logger.Warn("save cursor failed", zap.String("file_id", op), zap.Error(err))
		}
	}
}

func rotationWorker(ctx context.Context, in <-chan RotationJob, out chan<- FlushItem) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-in:
			if !ok {
				return nil
			}

			plain, err := Decrypt(job.OldKey.Key, job.Current.Ciphertext, job.Current.Nonce)
			if err != nil {
				return fmt.Errorf("decrypt chunk %s: %w", job.Current.UUID, err)
			}
			newKey, err := GenerateKey()
			if err != nil {
				return fmt.Errorf("generate key for chunk %s: %w", job.Current.UUID, err)
			}
			ciphertext, nonce, err := Encrypt(newKey, plain)
			if err != nil {
				return fmt.Errorf("encrypt chunk %s: %w", job.Current.UUID, err)
			}

			version := job.Current.Version + 1
			now := time.Now()
			item := FlushItem{
				Key: entity.ChunkKey{
					UUID:      job.Current.UUID,
					Key:       newKey,
					Version:   version,
					CreatedAt: now,
				},
				Data: entity.ChunkData{
					UUID:       job.Current.UUID,
					FileID:     job.Current.FileID,
					Ciphertext: ciphertext,
					Nonce:      nonce,
					Version:    version,
					CreatedAt:  now,
				},
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- item:
			}
		}
	}
}

type RetryConfig struct {
	Attempts int
	Backoff  time.Duration
}

func retryDo(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.Attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.Backoff):
			}
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return lastErr
		}
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", cfg.Attempts+1, lastErr)
}
