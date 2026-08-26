package v1

import (
	"context"
	"file-cipher-core/internal/handler/http/middleware"
	"file-cipher-core/pkg/logger"
	"io"
	"net/http"

	"github.com/google/uuid"
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
	logger   logger.Logger
}

func NewCipherHandler(cipher Cipher, rotator Rotator, decipher Decipher, log logger.Logger) *CipherHandler {
	return &CipherHandler{
		cipher:   cipher,
		rotator:  rotator,
		decipher: decipher,
		logger:   log,
	}
}

func (h *CipherHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /files", h.Encrypt)
	mux.HandleFunc("POST /files/{fileID}/rotate", h.Rotate)
	mux.HandleFunc("GET /files/{fileID}", h.Download)
	return middleware.Logging(h.logger)(mux)
}
