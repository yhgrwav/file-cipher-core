package repository

import (
	"context"
	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type DataRepository struct {
	db     *pgxpool.Pool
	logger *zap.Logger
}

func NewDataRepository(db *pgxpool.Pool, logger *zap.Logger) *DataRepository {
	return &DataRepository{db: db, logger: logger}
}

var (
	dataTableInfo = pgx.Identifier{"cipher", "chunk_data"}
	dataColumns   = []string{"uuid", "version", "ciphertext", "nonce", "created_at", "file_id"}
)

// GetLatestData возвращает по одной (самой свежей) записи чанка на каждый uuid из батча.
func (r *DataRepository) GetLatestData(ctx context.Context, ids []uuid.UUID) ([]entity.ChunkData, error) {
	query := `SELECT DISTINCT ON (uuid) uuid, version, ciphertext, nonce, file_id
			  FROM cipher.chunk_data
              WHERE uuid = ANY($1)
              ORDER BY uuid, version DESC`

	rows, err := r.db.Query(ctx, query, ids)
	if err != nil {
		r.logger.Error("error getting latest data", zap.Int("count", len(ids)), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var result []entity.ChunkData
	for rows.Next() {
		var d entity.ChunkData
		if err := rows.Scan(&d.UUID, &d.Version, &d.Ciphertext, &d.Nonce, &d.FileID); err != nil {
			r.logger.Error("error scanning data row", zap.Error(err))
			return nil, err
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating data rows", zap.Error(err))
		return nil, err
	}

	return result, nil
}

// GetDataByVersion возвращает запись чанка на конкретной версии
func (r *DataRepository) GetDataByVersion(ctx context.Context, id uuid.UUID, version int) (entity.ChunkData, error) {
	query := `SELECT uuid, version, ciphertext, nonce, file_id
			  FROM cipher.chunk_data
			  WHERE uuid = $1 AND version = $2`

	var d entity.ChunkData
	err := r.db.QueryRow(ctx, query, id, version).Scan(&d.UUID, &d.Version, &d.Ciphertext, &d.Nonce, &d.FileID)
	if err != nil {
		r.logger.Error("error getting data by version", zap.String("uuid", id.String()), zap.Int("version", version), zap.Error(err))
		return entity.ChunkData{}, err
	}
	return d, nil
}

// GetChunkUUIDsByFileID возвращает слайс айди чанков с пагинацией. Такой подход нужен, чтобы выстроить обратный стриминг
// данных юзеру, но при этом не забивать всю память сервера/тестовой машины данными, а постепенно отдавать куски информации.
// afterUUID - параметр пагинации.
func (r *DataRepository) GetChunkUUIDsByFileID(ctx context.Context, fileID uuid.UUID, afterUUID uuid.UUID, limit int) ([]uuid.UUID, error) {
	query := `SELECT DISTINCT uuid
			  FROM cipher.chunk_data
              WHERE file_id = $1 AND uuid > $2
              ORDER BY uuid ASC
              LIMIT $3`

	rows, err := r.db.Query(ctx, query, fileID, afterUUID, limit)
	if err != nil {
		r.logger.Error("error getting chunk uuids by file id", zap.String("file_id", fileID.String()), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			r.logger.Error("error scanning chunk uuid row", zap.Error(err))
			return nil, err
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating chunk uuid rows", zap.Error(err))
		return nil, err
	}

	return result, nil
}

// SaveData - батчевая вставка новых версий зашифрованных чанков.
func (r *DataRepository) SaveData(ctx context.Context, data []entity.ChunkData) error {
	_, err := r.db.CopyFrom(ctx, dataTableInfo, dataColumns,
		pgx.CopyFromRows(dataRowsHelper(data)),
	)
	if err != nil {
		r.logger.Error("failed to save chunk data", zap.Int("count", len(data)), zap.Error(err))
		return err
	}
	return nil
}

// хелпер для вставок в таблицу с данными
func dataRowsHelper(data []entity.ChunkData) [][]any {
	rows := make([][]any, 0, len(data))
	for _, value := range data {
		rows = append(rows, []any{value.UUID, value.Version, value.Ciphertext, value.Nonce, value.CreatedAt, value.FileID})
	}
	return rows
}
