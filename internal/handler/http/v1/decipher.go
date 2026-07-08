package v1

import (
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (h *CipherHandler) Download(w http.ResponseWriter, r *http.Request) {
	fileID, err := uuid.Parse(r.PathValue("fileID"))
	if err != nil {
		http.Error(w, "invalid file id", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileID.String()+".bin"))

	var out io.Writer = w
	if f, ok := w.(http.Flusher); ok {
		out = &flushWriter{w: w, f: f}
	}

	if err := h.decipher.StreamFile(r.Context(), fileID, out); err != nil {
		h.logger.Error("download failed", zap.String("file_id", fileID.String()), zap.Error(err))
		return
	}
}

type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw *flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.f.Flush()
	return n, err
}
