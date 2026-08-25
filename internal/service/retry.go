package service

import (
	"context"
	"fmt"
	"time"
)

type RetryConfig struct {
	Attempts int
	Backoff  time.Duration
}

// retryDo - хелпер, который делает n ретраев в принимаемой функции
func retryDo(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.Attempts; attempt++ {
		if attempt > 0 && !sleepCtx(ctx, cfg.Backoff) {
			return canceled(ctx, lastErr)
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return canceled(ctx, lastErr)
		}
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", cfg.Attempts+1, lastErr)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func canceled(ctx context.Context, lastErr error) error {
	if lastErr == nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w (last error: %v)", ctx.Err(), lastErr)
}

type loopBackoff struct {
	base time.Duration
	max  time.Duration
	cur  time.Duration
}

func (b *loopBackoff) wait(ctx context.Context) bool {
	// :-)))
	if b.cur == 0 {
		b.cur = b.base
	} else if b.cur < b.max {
		b.cur *= 2
		if b.cur > b.max {
			b.cur = b.max
		}
	}
	return sleepCtx(ctx, b.cur)
}

func (b *loopBackoff) reset() {
	b.cur = 0
}
