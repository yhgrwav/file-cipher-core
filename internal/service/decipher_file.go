package service

import (
	"context"
	"fmt"
	"io"

	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type (
	decipherDataReader interface {
		GetChunkUUIDsByFileID(ctx context.Context, fileID, afterUUID uuid.UUID, limit int) ([]uuid.UUID, error)
		GetLatestData(ctx context.Context, ids []uuid.UUID) ([]entity.ChunkData, error)
		GetDataByVersion(ctx context.Context, id uuid.UUID, version int) (entity.ChunkData, error)
	}
	decipherKeyReader interface {
		GetLatestKeys(ctx context.Context, ids []uuid.UUID) ([]entity.ChunkKey, error)
		GetKeyByVersion(ctx context.Context, id uuid.UUID, version int) (entity.ChunkKey, error)
	}
)

type Decipher struct {
	config DecipherConfig
	logger *zap.Logger
	data   decipherDataReader
	keys   decipherKeyReader
}

type DecipherConfig struct {
	PageSize int
	Retry    RetryConfig
}

func NewDecipher(cfg DecipherConfig, logger *zap.Logger, data decipherDataReader, keys decipherKeyReader) *Decipher {
	return &Decipher{
		config: cfg,
		logger: logger,
		data:   data,
		keys:   keys,
	}
}

func (d *Decipher) StreamFile(ctx context.Context, fileID uuid.UUID, wr io.Writer) error {
	d.logger.Info("stream file started", zap.String("file_id", fileID.String()))

	cursor := uuid.Nil
	var totalChunks int
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var ids []uuid.UUID
		if err := retryDo(ctx, d.config.Retry, func() error {
			var err error
			ids, err = d.data.GetChunkUUIDsByFileID(ctx, fileID, cursor, d.config.PageSize)
			return err
		}); err != nil {
			return fmt.Errorf("get chunk uuids: %w", err)
		}
		if len(ids) == 0 {
			d.logger.Info("stream file finished",
				zap.String("file_id", fileID.String()),
				zap.Int("total_chunks", totalChunks),
			)
			return nil
		}

		if err := d.streamPage(ctx, ids, wr); err != nil {
			return err
		}
		totalChunks += len(ids)
		cursor = ids[len(ids)-1]
	}
}

func (d *Decipher) streamPage(ctx context.Context, ids []uuid.UUID, wr io.Writer) error {
	var chunks []entity.ChunkData
	if err := retryDo(ctx, d.config.Retry, func() error {
		var err error
		chunks, err = d.data.GetLatestData(ctx, ids)
		return err
	}); err != nil {
		return fmt.Errorf("get latest data: %w", err)
	}
	chunkByUUID := make(map[uuid.UUID]entity.ChunkData, len(chunks))
	for _, c := range chunks {
		chunkByUUID[c.UUID] = c
	}

	var keys []entity.ChunkKey
	if err := retryDo(ctx, d.config.Retry, func() error {
		var err error
		keys, err = d.keys.GetLatestKeys(ctx, ids)
		return err
	}); err != nil {
		return fmt.Errorf("get latest keys: %w", err)
	}
	keyByUUID := make(map[uuid.UUID]entity.ChunkKey, len(keys))
	for _, k := range keys {
		keyByUUID[k.UUID] = k
	}

	for _, id := range ids {
		chunk, key := chunkByUUID[id], keyByUUID[id]
		if chunk.Version != key.Version {
			var err error
			chunk, key, err = d.consistentVersion(ctx, id, chunk, key)
			if err != nil {
				return err
			}
		}

		plain, err := d.decryptChunk(id, chunk, key)
		if err != nil {
			return err
		}
		if _, err := wr.Write(plain); err != nil {
			return fmt.Errorf("write chunk %s: %w", id, err)
		}
	}
	return nil
}

// consistentVersion возвращает консистентные записи из data и keys repository
func (d *Decipher) consistentVersion(ctx context.Context, id uuid.UUID, chunk entity.ChunkData, key entity.ChunkKey) (entity.ChunkData, entity.ChunkKey, error) {
	version := min(chunk.Version, key.Version)

	chunk, err := d.data.GetDataByVersion(ctx, id, version)
	if err != nil {
		return chunk, key, fmt.Errorf("get data version %d for chunk %s: %w", version, id, err)
	}
	key, err = d.keys.GetKeyByVersion(ctx, id, version)
	if err != nil {
		return chunk, key, fmt.Errorf("get key version %d for chunk %s: %w", version, id, err)
	}
	return chunk, key, nil
}

func (d *Decipher) decryptChunk(id uuid.UUID, chunk entity.ChunkData, key entity.ChunkKey) ([]byte, error) {
	if chunk.UUID == uuid.Nil {
		return nil, fmt.Errorf("data not found for chunk %s", id)
	}
	if key.UUID == uuid.Nil {
		return nil, fmt.Errorf("key not found for chunk %s", id)
	}
	if key.Version != chunk.Version {
		return nil, fmt.Errorf("version mismatch for chunk %s: key=%d data=%d", id, key.Version, chunk.Version)
	}

	plain, err := Decrypt(key.Key, chunk.Ciphertext, chunk.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decrypt chunk %s: %w", id, err)
	}
	return plain, nil
}
