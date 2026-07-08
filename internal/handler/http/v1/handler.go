package v1

import (
	"context"
	"io"
	"net/http"

	"file-cipher-core/internal/middleware"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Cipher interface {
	EncryptFile(ctx context.Context, fileID uuid.UUID, src io.Reader) error
}

type Rotator interface {
	Run(ctx context.Context, fileID uuid.UUID) error
}

type Decipher interface {
	StreamFile(ctx context.Context, fileID uuid.UUID, wr io.Writer) error
}

type CipherHandler struct {
	cipher   Cipher
	rotator  Rotator
	decipher Decipher
	logger   *zap.Logger
}

func NewCipherHandler(cipher Cipher, rotator Rotator, decipher Decipher, logger *zap.Logger) *CipherHandler {
	return &CipherHandler{
		cipher:   cipher,
		rotator:  rotator,
		decipher: decipher,
		logger:   logger,
	}
}

func (h *CipherHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /files", h.Encrypt)
	mux.HandleFunc("POST /files/{fileID}/rotate", h.Rotate)
	mux.HandleFunc("GET /files/{fileID}", h.Download)
	return middleware.Logging(h.logger)(mux)
}
