package service

import (
	"context"
	"file-cipher-core/internal/crypto"
	"fmt"
	"time"

	"file-cipher-core/internal/entity"
	"file-cipher-core/pkg/logger"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// worker pool
// rotationJob(curent data, oldkey)
// cursor based flusher
// parallel worker with common context
// 1. parallel worker (produce with retry)
// 2. delete old version
// 3. commit

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

type rotationJob struct {
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
	logger  logger.Logger
	cfg     RotatorConfig
}

func NewRotator(data rotationDataReader, keys rotationKeyReader, flusher *Flusher, cursors cursorStore, logger logger.Logger, cfg RotatorConfig) *Rotator {
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
	r.logger.Info("rotation started", "file_id", fileID.String())

	g, gctx := errgroup.WithContext(ctx)
	jobs := make(chan rotationJob, r.cfg.Workers)
	items := make(chan entity.FlushItem, r.cfg.Workers)

	g.Go(func() error {
		return r.flusher.Run(gctx, items)
	})

	g.Go(func() error {
		wg, wctx := errgroup.WithContext(gctx)
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
		return r.produce(gctx, fileID, jobs)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if err := r.deleteOldVersions(ctx, fileID); err != nil {
		return err
	}

	if err := r.cursors.Delete(ctx, fileID.String()); err != nil {
		r.logger.Warn("delete cursor failed", "file_id", fileID.String(), "error", err)
	}

	r.logger.Info("rotation finished", "file_id", fileID.String())
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

func (r *Rotator) produce(ctx context.Context, fileID uuid.UUID, out chan<- rotationJob) error {
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
			case out <- rotationJob{Current: chunk, OldKey: oldKey}:
			}
		}

		cursor = ids[len(ids)-1]
		if err := r.cursors.Save(ctx, op, cursor); err != nil {
			r.logger.Warn("save cursor failed", "file_id", op, "error", err)
		}
	}
}

func rotationWorker(ctx context.Context, in <-chan rotationJob, out chan<- entity.FlushItem) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-in:
			if !ok {
				return nil
			}

			plain, err := crypto.Decrypt(job.OldKey.Key, job.Current.Ciphertext, job.Current.Nonce)
			if err != nil {
				return fmt.Errorf("decrypt chunk %s: %w", job.Current.UUID, err)
			}
			newKey, err := crypto.GenerateKey()
			if err != nil {
				return fmt.Errorf("generate key for chunk %s: %w", job.Current.UUID, err)
			}
			ciphertext, nonce, err := crypto.Encrypt(newKey, plain)
			if err != nil {
				return fmt.Errorf("encrypt chunk %s: %w", job.Current.UUID, err)
			}

			version := job.Current.Version + 1
			now := time.Now()
			item := entity.FlushItem{
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
