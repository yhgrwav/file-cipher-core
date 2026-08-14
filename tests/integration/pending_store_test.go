//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
)

func testFlushItem() entity.FlushItem {
	id := uuid.New()
	return entity.FlushItem{
		Key: entity.ChunkKey{
			UUID:      id,
			Key:       []byte("secret-key"),
			Version:   1,
			CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		},
		Data: entity.ChunkData{
			UUID:       id,
			FileID:     uuid.New(),
			Ciphertext: []byte("ciphertext-bytes"),
			Nonce:      []byte("nonce-bytes"),
			Version:    1,
			CreatedAt:  time.Now().UTC().Truncate(time.Microsecond),
		},
	}
}

func TestPendingStore_AddBatchThenRead_RoundTrip(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)
	ctx := context.Background()

	item := testFlushItem()
	ids, err := store.AddBatch(ctx, []entity.FlushItem{item})
	if err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 id, got %d", len(ids))
	}

	got, err := store.Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 item read, got %d", len(got))
	}

	pending := got[0]
	if pending.ID != ids[0] {
		t.Errorf("read ID %q does not match AddBatch ID %q", pending.ID, ids[0])
	}
	if pending.Item.Key.UUID != item.Key.UUID {
		t.Errorf("key UUID mismatch: got %s want %s", pending.Item.Key.UUID, item.Key.UUID)
	}
	if pending.Item.Data.FileID != item.Data.FileID {
		t.Errorf("data FileID mismatch: got %s want %s", pending.Item.Data.FileID, item.Data.FileID)
	}
	if string(pending.Item.Data.Ciphertext) != string(item.Data.Ciphertext) {
		t.Errorf("ciphertext mismatch: got %q want %q", pending.Item.Data.Ciphertext, item.Data.Ciphertext)
	}
	if string(pending.Item.Data.Nonce) != string(item.Data.Nonce) {
		t.Errorf("nonce mismatch: got %q want %q", pending.Item.Data.Nonce, item.Data.Nonce)
	}
	if string(pending.Item.Key.Key) != string(item.Key.Key) {
		t.Errorf("key mismatch: got %q want %q", pending.Item.Key.Key, item.Key.Key)
	}
}

// TestPendingStore_ReadDoesNotReturnSameMessageTwice проверяет главное свойство consumer group:
// повторный Read без Ack не должен снова отдать уже выданное сообщение (оно всё ещё в PEL, а не
// доступно через ">").
func TestPendingStore_ReadDoesNotReturnSameMessageTwice(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)
	ctx := context.Background()

	if _, err := store.AddBatch(ctx, []entity.FlushItem{testFlushItem()}); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}

	first, err := store.Read(ctx, 10)
	if err != nil {
		t.Fatalf("first Read: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("expected 1 item on first read, got %d", len(first))
	}

	// второй Read с ">" не должен вернуть уже выданное сообщение
	second, err := store.Read(ctx, 10)
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("expected 0 items on second read (already delivered to this consumer), got %d", len(second))
	}
}

func TestPendingStore_AckRemovesFromPEL(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)
	ctx := context.Background()

	if _, err := store.AddBatch(ctx, []entity.FlushItem{testFlushItem()}); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	items, err := store.Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}

	if err := store.Ack(ctx, []string{items[0].ID}); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// после Ack запись не должна больше числиться зависшей в PEL - Claim с нулевым min-idle её не найдёт
	claimed, err := store.Claim(ctx, 0, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected 0 claimed items after Ack, got %d", len(claimed))
	}
}

// TestPendingStore_ClaimRecoversUnackedAfterMinIdle - основной сценарий, ради которого всё
// строилось: если процесс прочитал сообщение и упал до Ack, оно не теряется, а забирается Claim'ом
// после того как провисело в PEL дольше minIdle.
func TestPendingStore_ClaimRecoversUnackedAfterMinIdle(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)
	ctx := context.Background()

	item := testFlushItem()
	if _, err := store.AddBatch(ctx, []entity.FlushItem{item}); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}

	read, err := store.Read(ctx, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(read) != 1 {
		t.Fatalf("expected 1 item read, got %d", len(read))
	}
	// Ack намеренно не вызываем - имитируем падение процесса между Read и Ack

	time.Sleep(50 * time.Millisecond)

	claimed, err := store.Claim(ctx, 10*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed item (recovered from crash), got %d", len(claimed))
	}
	if claimed[0].Item.Key.UUID != item.Key.UUID {
		t.Errorf("claimed item UUID mismatch: got %s want %s", claimed[0].Item.Key.UUID, item.Key.UUID)
	}

	// после Claim запись выдана этому же consumer'у заново - Ack должен сработать и убрать её из PEL
	if err := store.Ack(ctx, []string{claimed[0].ID}); err != nil {
		t.Fatalf("Ack after claim: %v", err)
	}
	stillPending, err := store.Claim(ctx, 0, 10)
	if err != nil {
		t.Fatalf("final Claim: %v", err)
	}
	if len(stillPending) != 0 {
		t.Fatalf("expected 0 items left in PEL after ack, got %d", len(stillPending))
	}
}

func TestPendingStore_CursorInitIsIdempotent(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)
	ctx := context.Background()

	// первый CursorInit уже произошёл в newTestPendingStore; повторный вызов не должен падать
	// на BUSYGROUP
	if err := store.CursorInit(ctx); err != nil {
		t.Fatalf("second CursorInit should be a no-op, got error: %v", err)
	}
	if err := store.CursorInit(ctx); err != nil {
		t.Fatalf("third CursorInit should be a no-op, got error: %v", err)
	}
}
