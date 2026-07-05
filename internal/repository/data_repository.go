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
