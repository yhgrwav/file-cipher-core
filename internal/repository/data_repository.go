package repository

import (
	"context"
	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DataRepository struct {
	db *pgxpool.Pool
}

func NewDataRepository(db *pgxpool.Pool) *DataRepository {
	return &DataRepository{db: db}
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
		return nil, err
	}
	defer rows.Close()

	var result []entity.ChunkData
	for rows.Next() {
		var d entity.ChunkData
		if err := rows.Scan(&d.UUID, &d.Version, &d.Ciphertext, &d.Nonce, &d.FileID); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
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
		return nil, err
	}
	defer rows.Close()

	var result []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// DeleteOldData удаляет все версии чанков, кроме самой свежей
func (r *DataRepository) DeleteOldData(ctx context.Context, ids []uuid.UUID) error {
	query := `DELETE FROM cipher.chunk_data AS c
			  WHERE c.uuid = ANY($1)
			    AND c.version < (SELECT max(version) FROM cipher.chunk_data d WHERE d.uuid = c.uuid)`

	_, err := r.db.Exec(ctx, query, ids)
	if err != nil {
		return err
	}
	return nil
}

// SaveData - батчевая вставка новых версий зашифрованных чанков.
func (r *DataRepository) SaveData(ctx context.Context, data []entity.ChunkData) error {
	_, err := r.db.CopyFrom(ctx, dataTableInfo, dataColumns,
		pgx.CopyFromRows(dataRowsHelper(data)),
	)
	if err != nil {
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
