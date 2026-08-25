package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"file-cipher-core/internal/entity"
	"file-cipher-core/pkg/logger"
)

type fakePendingWriter struct {
	mu       sync.Mutex
	batches  [][]entity.FlushItem
	failN    int
	callN    int
	writeErr error
}

func (f *fakePendingWriter) AddBatch(_ context.Context, items []entity.FlushItem) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callN++
	if f.failN > 0 && f.callN <= f.failN {
		return nil, f.writeErr
	}
	cp := make([]entity.FlushItem, len(items))
	copy(cp, items)
	f.batches = append(f.batches, cp)
	ids := make([]string, len(items))
	for i := range ids {
		ids[i] = "id"
	}
	return ids, nil
}

func (f *fakePendingWriter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callN
}

func (f *fakePendingWriter) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func (f *fakePendingWriter) totalItems() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func testFlusherConfig() FlusherConfig {
	return FlusherConfig{
		BatchSize:            3,
		FlushTime:            50 * time.Millisecond,
		ShutdownFlushTimeout: 2 * time.Second,
		Retry:                RetryConfig{Attempts: 2, Backoff: 5 * time.Millisecond},
	}
}

func TestFlusher_FlushesOnBatchSize(t *testing.T) {
	writer := &fakePendingWriter{}
	f := NewFlusher(writer, logger.NewNop(), testFlusherConfig())

	in := make(chan entity.FlushItem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, in) }()

	for i := 0; i < 3; i++ {
		in <- entity.FlushItem{}
	}

	deadline := time.After(time.Second)
	for writer.batchCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("batch was not flushed by size trigger in time")
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got := writer.totalItems(); got != 3 {
		t.Fatalf("expected 3 items flushed, got %d", got)
	}

	cancel()
	<-done
}

func TestFlusher_FlushesOnTimer(t *testing.T) {
	writer := &fakePendingWriter{}
	cfg := testFlusherConfig()
	cfg.BatchSize = 100 // не даём сработать по размеру
	f := NewFlusher(writer, logger.NewNop(), cfg)

	in := make(chan entity.FlushItem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, in) }()

	in <- entity.FlushItem{}

	deadline := time.After(time.Second)
	for writer.batchCount() < 1 {
		select {
		case <-deadline:
			t.Fatal("batch was not flushed by timer in time")
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got := writer.totalItems(); got != 1 {
		t.Fatalf("expected 1 item flushed, got %d", got)
	}

	cancel()
	<-done
}

// TestFlusher_ClosedChannelFlushesTailAndReturns проверяет починенный баг busy-loop: закрытие
// канала должно дописать остаток и вернуть управление, а не крутиться вечно.
func TestFlusher_ClosedChannelFlushesTailAndReturns(t *testing.T) {
	writer := &fakePendingWriter{}
	cfg := testFlusherConfig()
	cfg.BatchSize = 100
	f := NewFlusher(writer, logger.NewNop(), cfg)

	in := make(chan entity.FlushItem, 2)
	in <- entity.FlushItem{}
	in <- entity.FlushItem{}
	close(in)

	done := make(chan error, 1)
	go func() { done <- f.Run(context.Background(), in) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on graceful close, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after input channel closed (busy-loop regression)")
	}

	if got := writer.totalItems(); got != 2 {
		t.Fatalf("expected tail of 2 items flushed on close, got %d", got)
	}
}

func TestFlusher_ShutdownFlushesTail(t *testing.T) {
	writer := &fakePendingWriter{}
	cfg := testFlusherConfig()
	cfg.BatchSize = 100
	cfg.FlushTime = time.Hour
	f := NewFlusher(writer, logger.NewNop(), cfg)

	in := make(chan entity.FlushItem)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, in) }()

	in <- entity.FlushItem{}
	in <- entity.FlushItem{}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	if got := writer.totalItems(); got != 2 {
		t.Fatalf("expected 2 items flushed on shutdown, got %d", got)
	}
}

func TestFlusher_RetriesOnWriteFailureThenSucceeds(t *testing.T) {
	writer := &fakePendingWriter{failN: 2, writeErr: errors.New("boom")}
	cfg := testFlusherConfig()
	cfg.BatchSize = 1
	f := NewFlusher(writer, logger.NewNop(), cfg)

	in := make(chan entity.FlushItem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, in) }()

	in <- entity.FlushItem{}

	deadline := time.After(time.Second)
	for writer.batchCount() < 1 {
		select {
		case <-deadline:
			t.Fatalf("batch never succeeded after retries, calls so far: %d", writer.callCount())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if writer.callCount() != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got %d", writer.callCount())
	}

	cancel()
	<-done
}

func TestFlusher_ReturnsErrorAfterExhaustingRetries(t *testing.T) {
	writeErr := errors.New("permanent failure")
	writer := &fakePendingWriter{failN: 1000, writeErr: writeErr}
	cfg := testFlusherConfig()
	cfg.BatchSize = 1
	cfg.Retry = RetryConfig{Attempts: 1, Backoff: time.Millisecond}
	f := NewFlusher(writer, logger.NewNop(), cfg)

	in := make(chan entity.FlushItem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, in) }()

	in <- entity.FlushItem{}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after exhausting retries, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return an error in time")
	}
}
