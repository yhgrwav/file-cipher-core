package service

import (
	"context"
	"errors"
	"file-cipher-core/internal/crypto"
	"fmt"
	"io"
	"time"

	"file-cipher-core/internal/entity"
	"file-cipher-core/pkg/logger"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// worker pool
// 1. parallel read
// 2. encrypt chunk
// 3. batch append
// 4. EOF -> flush

type CipherConfig struct {
	ChunkSize int
	Workers   int
}

type Cipher struct {
	flusher *Flusher
	logger  logger.Logger
	cfg     CipherConfig
}

func NewCipher(flusher *Flusher, logger logger.Logger, cfg CipherConfig) *Cipher {
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
	c.logger.Info("encrypt file started", "file_id", fileID.String())

	g, ctx := errgroup.WithContext(ctx)
	jobs := make(chan encryptJob, c.cfg.Workers)
	items := make(chan entity.FlushItem, c.cfg.Workers)

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

	c.logger.Info("encrypt file finished", "file_id", fileID.String())
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
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read chunk: %w", err)
		}
	}
}

func encryptWorker(ctx context.Context, in <-chan encryptJob, out chan<- entity.FlushItem) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-in:
			if !ok {
				return nil
			}

			key, err := crypto.GenerateKey()
			if err != nil {
				return fmt.Errorf("generate key for chunk %s: %w", job.chunkID, err)
			}
			ciphertext, nonce, err := crypto.Encrypt(key, job.plaintext)
			if err != nil {
				return fmt.Errorf("encrypt chunk %s: %w", job.chunkID, err)
			}

			now := time.Now()
			item := entity.FlushItem{
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
