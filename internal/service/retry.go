package service

import (
	"context"
	"fmt"
	"time"
)

// withRetry реализует линейный бэкофф
func withRetry(ctx context.Context, retries int, backoff time.Duration, writeBatch func() error) error {
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(backoff):
			}
		}
		lastErr = writeBatch()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return lastErr
		}
	}
	return fmt.Errorf("write failed after %d retries: %w", retries+1, lastErr)
}