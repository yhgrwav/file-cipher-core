package v1

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// trackingWriter - приватная структура, хранящая состояние "успели ли мы записать хоть один байт" (
// если written == true - передаём соединение Hijacker и обрываем, если нет - кидаем http.Error(500)
type trackingWriter struct {
	wr      http.ResponseWriter
	f       http.Flusher
	written bool
}

func (h *CipherHandler) Download(w http.ResponseWriter, r *http.Request) {
	fileID, err := uuid.Parse(r.PathValue("fileID"))
	if err != nil {
		http.Error(w, "invalid file id", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileID.String()+".bin"))

	tw := &trackingWriter{wr: w}

	// если имплементируем интерфейсу и можем вызывать метод Flush() - ок. подставляем флашер и двигаемся дальше
	// если нет - явно обрабатываем этот сценарий, чтоб приложение не поломалось. без явного Flush() данные тоже будут
	// грузиться, но рывками. не критично, но главное безопасно, т.к. есть явная проверка на ok || !ok
	if f, ok := w.(http.Flusher); ok {
		tw.f = f
	}

	if err := h.decipher.StreamFile(r.Context(), fileID, tw); err != nil {
		h.logger.Error("download failed", zap.String("file_id", fileID.String()), zap.Error(err))

		// если ни единого байта не записалось - это internal ошибка и можно сразу отдать 500 статускод
		if !tw.written {
			http.Error(w, "download failed", http.StatusInternalServerError)
			return
		}

		// когда часть тела уже отправлена клиенту, статус 200 и заголовки поменять больше нельзя.
		// если просто сделать return, net/http сам корректно допишет завершающий чанк,
		// и клиент получит "успешный", но обрезанный файл без признаков ошибки.
		// Hijacker забирает управление соединением у сервера, чтобы мы могли
		// резко оборвать его (conn.Close()) - тогда клиент получит явную ошибку
		// транспорта вместо испорченного файла.
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			h.logger.Error("hijack failed", zap.String("file_id", fileID.String()), zap.Error(err))
			return
		}
		conn.Close()
		return
	}
}

func (tw *trackingWriter) Write(p []byte) (int, error) {
	// если запись вызвалась - уже необходимо передать состояние, т.к. заголовки уже отправлены
	tw.written = true
	n, err := tw.wr.Write(p)

	if tw.f != nil {
		tw.f.Flush()
	}
	return n, err
}
