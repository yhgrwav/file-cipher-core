//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"file-cipher-core/internal/entity"
	"file-cipher-core/internal/repository"
	"file-cipher-core/internal/service"
	"file-cipher-core/pkg/logger"
)

func workerCfg() service.PendingWorkerConfig {
	return service.PendingWorkerConfig{
		Amount:            100,
		ClaimInterval:     100 * time.Millisecond,
		ClaimMinIdle:      50 * time.Millisecond,
		WriteRetries:      3,
		WriteRetryBackoff: 50 * time.Millisecond,
	}
}

// TestPendingWorker_EndToEnd_ReadWritesToPostgres прогоняет полный путь без единого мока:
// AddBatch в Redis -> PendingWorker вычитывает через consumer group -> пишет в реальные
// key-db/data-db -> подтверждает Ack. Это тест ровно той гарантии, ради которой строился весь
// Redis-буфер вместо прямой записи в Postgres.
func TestPendingWorker_EndToEnd_ReadWritesToPostgres(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)

	keyPool := newTestKeyPool(t)
	dataPool := newTestDataPool(t)
	keyRepo := repository.NewKeyRepository(keyPool)
	dataRepo := repository.NewDataRepository(dataPool)

	worker := service.NewPendingWorker(store, dataRepo, keyRepo, logger.NewNop(), workerCfg())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = worker.Run(ctx) }()

	item := testFlushItem()
	if _, err := store.AddBatch(context.Background(), []entity.FlushItem{item}); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var gotKey entity.ChunkKey
	for time.Now().Before(deadline) {
		k, err := keyRepo.GetKeyByVersion(context.Background(), item.Key.UUID, item.Key.Version)
		if err == nil {
			gotKey = k
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if gotKey.UUID != item.Key.UUID {
		t.Fatalf("key was not written to postgres within timeout")
	}

	gotData, err := dataRepo.GetDataByVersion(context.Background(), item.Data.UUID, item.Data.Version)
	if err != nil {
		t.Fatalf("data was not written to postgres: %v", err)
	}
	if string(gotData.Ciphertext) != string(item.Data.Ciphertext) {
		t.Errorf("ciphertext mismatch: got %q want %q", gotData.Ciphertext, item.Data.Ciphertext)
	}

	// дожидаемся Ack, чтобы убедиться, что запись не осталась в PEL навечно
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		claimed, err := store.Claim(context.Background(), 0, 10)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(claimed) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("record was never acked - still claimable from PEL after successful write")
}

// TestPendingWorker_ClaimRecoversAfterSimulatedCrash - имитация того, что должно произойти
// завтра при ручном тесте через docker-compose: процесс успел Read, но не успел Ack (аналог
// падения между чтением и записью), затем новый воркер (как после рестарта) поднимает Claim
// и дописывает данные, ничего не потеряв.
func TestPendingWorker_ClaimRecoversAfterSimulatedCrash(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)

	keyPool := newTestKeyPool(t)
	dataPool := newTestDataPool(t)
	keyRepo := repository.NewKeyRepository(keyPool)
	dataRepo := repository.NewDataRepository(dataPool)

	item := testFlushItem()
	if _, err := store.AddBatch(context.Background(), []entity.FlushItem{item}); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}

	// "упавший" воркер: прочитал, но так и не заакал и не записал
	if _, err := store.Read(context.Background(), 10); err != nil {
		t.Fatalf("simulated crashed Read: %v", err)
	}

	time.Sleep(60 * time.Millisecond) // пережидаем ClaimMinIdle

	worker := service.NewPendingWorker(store, dataRepo, keyRepo, logger.NewNop(), workerCfg())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = worker.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := keyRepo.GetKeyByVersion(context.Background(), item.Key.UUID, item.Key.Version)
		if err == nil {
			return // восстановлено и записано - тест пройден
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("record left unacked by a crashed reader was never recovered via Claim")
}

func TestFlusher_WritesIntoPendingStore(t *testing.T) {
	rdb := newTestRedis(t)
	store := newTestPendingStore(t, rdb)

	flusher := service.NewFlusher(store, logger.NewNop(), service.FlusherConfig{
		BatchSize:            10,
		FlushTime:            100 * time.Millisecond,
		ShutdownFlushTimeout: 2 * time.Second,
		WriteRetries:         2,
		WriteRetryBackoff:    50 * time.Millisecond,
	})

	in := make(chan entity.FlushItem)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- flusher.Run(ctx, in) }()

	item := testFlushItem()
	in <- item

	deadline := time.Now().Add(2 * time.Second)
	var read []entity.PendingItem
	for time.Now().Before(deadline) {
		got, err := store.Read(context.Background(), 10)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(got) > 0 {
			read = got
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(read) != 1 {
		t.Fatalf("expected item to land in redis via Flusher, got %d items", len(read))
	}
	if read[0].Item.Data.UUID != item.Data.UUID {
		t.Errorf("UUID mismatch: got %s want %s", read[0].Item.Data.UUID, item.Data.UUID)
	}

	cancel()
	<-done
}
