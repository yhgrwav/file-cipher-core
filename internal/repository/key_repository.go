package repository

import (
	"context"
	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type KeyRepository struct {
	db     *pgxpool.Pool
	logger *zap.Logger
}

func NewKeyRepository(db *pgxpool.Pool, logger *zap.Logger) *KeyRepository {
	return &KeyRepository{db: db, logger: logger}
}

var (
	keyColumns    = []string{"uuid", "version", "key", "created_at"}
	keysTableInfo = pgx.Identifier{"cipher", "chunk_keys"}
)

// GetLatestKeys возвращает по одной (самой свежей) записи ключа на каждый uuid из батча.
func (r *KeyRepository) GetLatestKeys(ctx context.Context, ids []uuid.UUID) ([]entity.ChunkKey, error) {
	// логика запроса:
	// выбирает поля с помощью DISTINCT ON (uuid)
	// если мы получаем несколько дубликатов uuid - сортируем version DESC (от большего к меньшему)
	// и на выходе получаем самые актуальные версии каждого чанка
	query := `SELECT DISTINCT ON (uuid) uuid, version, key, created_at
			  FROM cipher.chunk_keys
              WHERE uuid = ANY($1)
              ORDER BY uuid, version DESC`

	rows, err := r.db.Query(ctx, query, ids)
	if err != nil {
		r.logger.Error("error getting latest keys", zap.Int("count", len(ids)), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var result []entity.ChunkKey
	for rows.Next() {
		var k entity.ChunkKey
		if err := rows.Scan(&k.UUID, &k.Version, &k.Key, &k.CreatedAt); err != nil {
			r.logger.Error("error scanning key row", zap.Error(err))
			return nil, err
		}
		result = append(result, k)
	}
	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating key rows", zap.Error(err))
		return nil, err
	}

	return result, nil
}

// SaveKeys - батчевая вставка новых версий ключей.
func (r *KeyRepository) SaveKeys(ctx context.Context, keys []entity.ChunkKey) error {
	// в общем CopyFrom для меня стал открытием, т.к. я всегда использовал pgx.Batch, который как я понял рассчитан больше
	// на ситуации, когда одним батчем мы передаём в разные таблицы разные записи, из-за чего для каждого запроса создаётся
	// вот этот пайплайн, который видно при EXPLAIN ANALYZE, а CopyFrom решает эту проблему тем, что мы заранее единожды
	// описываем в pgx.Identifier схему и таблицу, куда мы будем записывать данные, затем передаём []string с названиеями
	// колонок и передаём [][]any из наших данных, которые библиотека преобразует в бесполые записи и записывает эффективне,
	// не тратя ресурсы на то, чтобы продумать что, куда и как записывать.
	_, err := r.db.CopyFrom(ctx, keysTableInfo, keyColumns, pgx.CopyFromRows(keyRowsHelper(keys)))
	if err != nil {
		r.logger.Error("failed to save chunk keys", zap.Int("count", len(keys)), zap.Error(err))
		return err
	}
	return nil
}

// хелпер для вставок в таблицу с ключами
func keyRowsHelper(data []entity.ChunkKey) [][]any {
	rows := make([][]any, 0, len(data))
	for _, value := range data {
		rows = append(rows, []any{value.UUID, value.Version, value.Key, value.CreatedAt})
	}
	return rows
}
