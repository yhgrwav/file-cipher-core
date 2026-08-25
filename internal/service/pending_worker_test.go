package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"file-cipher-core/internal/entity"
	"file-cipher-core/pkg/logger"

	"github.com/google/uuid"
)

type fakeKeysRepo struct {
	mu      sync.Mutex
	saved   []entity.ChunkKey
	failN   int
	callN   int
	saveErr error
}

func (r *fakeKeysRepo) SaveKeys(_ context.Context, keys []entity.ChunkKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.callN++
	if r.failN > 0 && r.callN <= r.failN {
		return r.saveErr
	}
	r.saved = append(r.saved, keys...)
	return nil
}

func (r *fakeKeysRepo) savedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saved)
}

type fakeDataRepo struct {
	mu      sync.Mutex
	saved   []entity.ChunkData
	failN   int
	callN   int
	saveErr error
}

func (r *fakeDataRepo) SaveData(_ context.Context, data []entity.ChunkData) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.callN++
	if r.failN > 0 && r.callN <= r.failN {
		return r.saveErr
	}
	r.saved = append(r.saved, data...)
	return nil
}

func (r *fakeDataRepo) savedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saved)
}

type fakePendingStore struct {
	mu       sync.Mutex
	readOnce []entity.PendingItem
	readDone bool
	acked    [][]string
	ackErr   error
}

func (s *fakePendingStore) Read(ctx context.Context, _ int) ([]entity.PendingItem, error) {
	s.mu.Lock()
	if !s.readDone {
		s.readDone = true
		items := s.readOnce
		s.mu.Unlock()
		return items, nil
	}
	s.mu.Unlock()
	// имитируем блокирующий Read с Block-таймаутом: ждём отмены контекста
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *fakePendingStore) Ack(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ackErr != nil {
		return s.ackErr
	}
	s.acked = append(s.acked, ids)
	return nil
}

func (s *fakePendingStore) Claim(ctx context.Context, _ time.Duration, _ int) ([]entity.PendingItem, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *fakePendingStore) ackedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, batch := range s.acked {
		out = append(out, batch...)
	}
	return out
}

func testPendingItem(id string) entity.PendingItem {
	return entity.PendingItem{
		ID: id,
		Item: entity.FlushItem{
			Key:  entity.ChunkKey{UUID: uuid.New(), Version: 1, Key: []byte("key")},
			Data: entity.ChunkData{UUID: uuid.New(), FileID: uuid.New(), Version: 1, Ciphertext: []byte("ct"), Nonce: []byte("n")},
		},
	}
}

func testPendingWorkerConfig() PendingWorkerConfig {
	return PendingWorkerConfig{
		Amount:            100,
		ClaimInterval:     time.Hour,
		ClaimMinIdle:      time.Minute,
		WriteRetries:      2,
		WriteRetryBackoff: 5 * time.Millisecond,
	}
}

func TestPendingWorker_ProcessAcksCorrectIDs(t *testing.T) {
	store := &fakePendingStore{}
	keys := &fakeKeysRepo{}
	data := &fakeDataRepo{}
	w := NewPendingWorker(store, data, keys, logger.NewNop(), testPendingWorkerConfig())

	items := []entity.PendingItem{testPendingItem("1-0"), testPendingItem("2-0")}

	if err := w.process(context.Background(), items); err != nil {
		t.Fatalf("process failed: %v", err)
	}

	if keys.savedCount() != 2 || data.savedCount() != 2 {
		t.Fatalf("expected 2 keys and 2 data saved, got keys=%d data=%d", keys.savedCount(), data.savedCount())
	}

	acked := store.ackedIDs()
	if len(acked) != 2 || acked[0] != "1-0" || acked[1] != "2-0" {
		t.Fatalf("expected ack for exactly [1-0 2-0], got %v", acked)
	}
}

// TestPendingWorker_PartialFailureDoesNotAck проверяет ключевой инвариант гарантии доставки:
// если запись в Postgres не удалась (после исчерпания ретраев), Ack не вызывается вовсе,
// и запись остаётся в PEL для последующего Claim.
func TestPendingWorker_PartialFailureDoesNotAck(t *testing.T) {
	store := &fakePendingStore{}
	keys := &fakeKeysRepo{}
	data := &fakeDataRepo{failN: 1000, saveErr: errors.New("pg down")}
	cfg := testPendingWorkerConfig()
	cfg.WriteRetries = 1
	cfg.WriteRetryBackoff = time.Millisecond
	w := NewPendingWorker(store, data, keys, logger.NewNop(), cfg)

	items := []entity.PendingItem{testPendingItem("1-0")}

	if err := w.process(context.Background(), items); err == nil {
		t.Fatal("expected error from process when SaveData permanently fails")
	}

	if len(store.acked) != 0 {
		t.Fatalf("expected no Ack calls on write failure, got %v", store.acked)
	}
}

func TestPendingWorker_RetriesTransientWriteFailure(t *testing.T) {
	store := &fakePendingStore{}
	keys := &fakeKeysRepo{failN: 2, saveErr: errors.New("transient")}
	data := &fakeDataRepo{}
	cfg := testPendingWorkerConfig()
	cfg.WriteRetries = 3
	cfg.WriteRetryBackoff = time.Millisecond
	w := NewPendingWorker(store, data, keys, logger.NewNop(), cfg)

	items := []entity.PendingItem{testPendingItem("1-0")}

	if err := w.process(context.Background(), items); err != nil {
		t.Fatalf("expected process to succeed after retries, got %v", err)
	}

	if len(store.acked) != 1 {
		t.Fatalf("expected exactly 1 ack call after eventual success, got %d", len(store.acked))
	}
}

func TestPendingWorker_ReadLoopProcessesThenStopsOnCancel(t *testing.T) {
	store := &fakePendingStore{readOnce: []entity.PendingItem{testPendingItem("1-0")}}
	keys := &fakeKeysRepo{}
	data := &fakeDataRepo{}
	w := NewPendingWorker(store, data, keys, logger.NewNop(), testPendingWorkerConfig())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.readLoop(ctx) }()

	deadline := time.After(time.Second)
	for keys.savedCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("readLoop never processed the item read on first Read call")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("readLoop did not return after cancel")
	}
}
