package v1

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

type encryptResponse struct {
	FileID string `json:"file_id"`
}

func (h *CipherHandler) Encrypt(w http.ResponseWriter, r *http.Request) {
	fileID, err := uuid.NewV7()
	if err != nil {
		h.logger.Error("new file id", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := h.cipher.EncryptFile(r.Context(), fileID, r.Body); err != nil {
		h.logger.Error("encrypt failed", "file_id", fileID.String(), "error", err)
		http.Error(w, "encryption failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(encryptResponse{FileID: fileID.String()}); err != nil {
		h.logger.Error("write response failed", "file_id", fileID.String(), "error", err)
	}
}
