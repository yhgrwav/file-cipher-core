package middleware

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func Logging(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			logger.Info("request", zap.String("method", r.Method), zap.String("path", r.URL.Path), zap.Int("status", rec.status), zap.Duration("duration", time.Since(start)))
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// проблема:
// если в интерфейсе лежит ещё один интерфейс, то мы можем дотянуться только до тех методов, которые явно описаны в
// встроенном интерфейсе.
// пример:
// type someThing interface {
// Something2 (interface)
// }
// type Something2 interface {
// bark() <- проблема
// }
//
// суть: если сущность, имплементирующая интерфейсу Something2 реализует bark(), то мы его можем вызвать, но в конкретном
// случае нам нужен не только bark(), но и ещё условный meow(), который не описан в интерфейсе, но является методом
// сущности, которая имплемементирует интерфейсу.
// решение:
// сделать явный type assertion, который безопасно вытягивает нужный нам интерфейс у структуры и если всё ок, то
// возвращает нужные нам инструменты для работы, а если нет - безопасно возвращает крутую ошибку.
//
// возвращаясь к примеру выше: someThing - наша обёртка, которую мы будем использовать в нашем коде, а встроенный интерфейс -
// интерфейс стандартной либы, откуда нужно вытянуть нужный функционал.
//
// итого:
// этот метод и решает вышеописанную проблему и чинит баг, который в decipher.go (там, где обрабатывается ошибка стриминга
// файла юзеру) всегда бы возвращал при проверке  hj, ok := w.(http.Hijacker); if !ok { return } не ok, из-за того,
// что не видел бы необходимый для рабоыт метод.

// Hijack - функция, возвращающая инструменты перехвата TCP-соединения, чтобы в случае возникновения ошибки явно
// отменить загрузку файла с ошибкой, а не загружать битый файл без ошибки
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("resp writer does not implement http.Hijacker ✌(-‿-)✌")
	}
	return hj.Hijack()
}
