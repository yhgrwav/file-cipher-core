package service

import (
	"context"
	"fmt"
	"io"

	"file-cipher-core/internal/entity"
	"file-cipher-core/internal/repository"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Decipher struct {
	config DecipherConfig            // настройки расшифровщика
	logger *zap.Logger               // логгер
	data   repository.DataRepository // доступ к БД с данными
	keys   repository.KeyRepository  // доступ к БД с ключами
}

type DecipherConfig struct {
	// PageSize — размер keyset-страницы при обходе чанков файла.
	PageSize int
}

func NewDecipher(cfg DecipherConfig, logger *zap.Logger, data repository.DataRepository, keys repository.KeyRepository) *Decipher {
	return &Decipher{
		config: cfg,
		logger: logger,
		data:   data,
		keys:   keys,
	}
}

// StreamFile собирает исходный файл и пишет его в writer юзеру
func (d *Decipher) StreamFile(ctx context.Context, fileID uuid.UUID, wr io.Writer) error {
	// т.к. в любой нормальной системе с высоким рпс важно экономить ресурсы, то я бы не добавлял этот лог, но в рамках
	// тестового, которое вряд ли будет гоняться в каком-то нагрузочном тестировании, решил добавить небольшую запись.
	d.logger.Info("stream file started", zap.String("file_id", fileID.String()))

	cursor := uuid.Nil
	var totalChunks int
	for {
		if err := ctx.Err(); err != nil { // клиент отвалился || запрос отменён
			return err
		}

		ids, err := d.data.GetChunkUUIDsByFileID(ctx, fileID, cursor, d.config.PageSize)
		if err != nil {
			return fmt.Errorf("get chunk uuids: %w", err)
		}
		if len(ids) == 0 {
			d.logger.Info("stream file finished",
				zap.String("file_id", fileID.String()),
				zap.Int("total_chunks", totalChunks),
			)
			return nil // файл собран, это основное условия выхода
		}

		if err := d.streamPage(ctx, ids, wr); err != nil {
			return err
		}
		totalChunks += len(ids)
		cursor = ids[len(ids)-1] // сдвигаем курсор на последний UUID полученной страницы из GetChunkUUIDsByFileID после выполнения логики

		d.logger.Debug("page streamed", zap.String("file_id", fileID.String()), zap.Int("page_size", len(ids)), zap.String("cursor", cursor.String()))
	}
}

// streamPage расшифровывает одну страницу чанков и пишет её в writer по порядку
func (d *Decipher) streamPage(ctx context.Context, ids []uuid.UUID, wr io.Writer) error {
	// data (БД2) и keys (БД1) приходят в произвольном порядке — индексируем по UUID,
	// чтобы дальше matchить их с эталонным порядком ids за O(1). (это идея и реализация курсора, я уже не могу думать, а мне ещё ротацию дописывать, но в целом всё понятно)
	chunks, err := d.data.GetLatestData(ctx, ids)
	if err != nil {
		return fmt.Errorf("get latest data: %w", err)
	}
	chunkByUUID := make(map[uuid.UUID]entity.ChunkData, len(chunks))
	for _, c := range chunks {
		chunkByUUID[c.UUID] = c
	}

	keys, err := d.keys.GetLatestKeys(ctx, ids)
	if err != nil {
		return fmt.Errorf("get latest keys: %w", err)
	}
	keyByUUID := make(map[uuid.UUID]entity.ChunkKey, len(keys))
	for _, k := range keys {
		keyByUUID[k.UUID] = k
	}

	for _, id := range ids {
		plain, err := d.decryptChunk(id, chunkByUUID[id], keyByUUID[id])
		if err != nil {
			return err
		}
		if _, err := wr.Write(plain); err != nil {
			return fmt.Errorf("write chunk %s: %w", id, err)
		}
	}
	return nil
}

// decryptChunk расшифровывает один чанк, сверяя версии ключа и данных.
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
