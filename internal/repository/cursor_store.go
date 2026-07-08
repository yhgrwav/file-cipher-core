package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type CursorStore struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewCursorStore(rdb *redis.Client, ttl time.Duration) *CursorStore {
	return &CursorStore{rdb: rdb, ttl: ttl}
}

func (s *CursorStore) key(op string) string {
	return "cursor:" + op
}

func (s *CursorStore) Save(ctx context.Context, op string, cursor uuid.UUID) error {
	return s.rdb.Set(ctx, s.key(op), cursor.String(), s.ttl).Err()
}

func (s *CursorStore) Load(ctx context.Context, op string) (uuid.UUID, bool, error) {
	val, err := s.rdb.Get(ctx, s.key(op)).Result()
	if errors.Is(err, redis.Nil) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	id, err := uuid.Parse(val)
	if err != nil {
		return uuid.Nil, false, err
	}
	return id, true, nil
}

func (s *CursorStore) Delete(ctx context.Context, op string) error {
	return s.rdb.Del(ctx, s.key(op)).Err()
}
