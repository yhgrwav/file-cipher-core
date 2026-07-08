package service

import (
	"context"
	"fmt"
	"io"
	"time"

	"file-cipher-core/internal/entity"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type CipherConfig struct {
	ChunkSize int
	Workers   int
}

type Cipher struct {
	flusher *Flusher
	logger  *zap.Logger
	cfg     CipherConfig
}

func NewCipher(flusher *Flusher, logger *zap.Logger, cfg CipherConfig) *Cipher {
	return &Cipher{
		flusher: flusher,
		logger:  logger,
		cfg:     cfg,
	}
}

type encryptJob struct {
	fileID    uuid.UUID
	chunkID   uuid.UUID
	plaintext []byte
}

func (c *Cipher) EncryptFile(ctx context.Context, fileID uuid.UUID, src io.Reader) error {
	c.logger.Info("encrypt file started", zap.String("file_id", fileID.String()))

	g, ctx := errgroup.WithContext(ctx)
	jobs := make(chan encryptJob, c.cfg.Workers)
	items := make(chan FlushItem, c.cfg.Workers)

	g.Go(func() error {
		return c.flusher.Run(ctx, items)
	})

	g.Go(func() error {
		wg, wctx := errgroup.WithContext(ctx)
		for i := 0; i < c.cfg.Workers; i++ {
			wg.Go(func() error {
				return encryptWorker(wctx, jobs, items)
			})
		}
		err := wg.Wait()
		close(items)
		return err
	})

	g.Go(func() error {
		defer close(jobs)
		return c.read(ctx, fileID, src, jobs)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	c.logger.Info("encrypt file finished", zap.String("file_id", fileID.String()))
	return nil
}

func (c *Cipher) read(ctx context.Context, fileID uuid.UUID, src io.Reader, out chan<- encryptJob) error {
	for {
		buf := make([]byte, c.cfg.ChunkSize)
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			chunkID, gerr := uuid.NewV7()
			if gerr != nil {
				return fmt.Errorf("new chunk uuid: %w", gerr)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- encryptJob{fileID: fileID, chunkID: chunkID, plaintext: buf[:n]}:
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return fmt.Errorf("read chunk: %w", err)
		}
	}
}

func encryptWorker(ctx context.Context, in <-chan encryptJob, out chan<- FlushItem) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-in:
			if !ok {
				return nil
			}

			key, err := GenerateKey()
			if err != nil {
				return fmt.Errorf("generate key for chunk %s: %w", job.chunkID, err)
			}
			ciphertext, nonce, err := Encrypt(key, job.plaintext)
			if err != nil {
				return fmt.Errorf("encrypt chunk %s: %w", job.chunkID, err)
			}

			now := time.Now()
			item := FlushItem{
				Key: entity.ChunkKey{
					UUID:      job.chunkID,
					Key:       key,
					Version:   1,
					CreatedAt: now,
				},
				Data: entity.ChunkData{
					UUID:       job.chunkID,
					FileID:     job.fileID,
					Ciphertext: ciphertext,
					Nonce:      nonce,
					Version:    1,
					CreatedAt:  now,
				},
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- item:
			}
		}
	}
}
