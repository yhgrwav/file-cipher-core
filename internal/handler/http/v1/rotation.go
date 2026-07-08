package v1

import (
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (h *CipherHandler) Rotate(w http.ResponseWriter, r *http.Request) {
	fileID, err := uuid.Parse(r.PathValue("fileID"))
	if err != nil {
		http.Error(w, "invalid file id", http.StatusBadRequest)
		return
	}

	if err := h.rotator.Run(r.Context(), fileID); err != nil {
		h.logger.Error("rotate failed", zap.String("file_id", fileID.String()), zap.Error(err))
		http.Error(w, "rotation failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
