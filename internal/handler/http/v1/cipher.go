package v1

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (h *CipherHandler) Encrypt(w http.ResponseWriter, r *http.Request) {
	fileID, err := uuid.NewV7()
	if err != nil {
		h.logger.Error("new file id", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if err := h.cipher.EncryptFile(r.Context(), fileID, r.Body); err != nil {
		h.logger.Error("encrypt failed", zap.String("file_id", fileID.String()), zap.Error(err))
		http.Error(w, "encryption failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"file_id":%q}`, fileID.String())
}
