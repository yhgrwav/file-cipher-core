package service

import (
	"context"
	"fmt"
	"time"

	"file-cipher-core/internal/entity"

	"go.uber.org/zap"
)

type (
	keyWriter interface {
		SaveKeys(ctx context.Context, keys []entity.ChunkKey) error
	}
	dataWriter interface {
		SaveData(ctx context.Context, data []entity.ChunkData) error
	}
)

// FlushItem - результат работы воркера: одна пара (ключ в БД1, шифртекст в БД2)
// для одного чанка. Flusher копит такие пары и пишет их в базы пачками.
type FlushItem struct {
	Key  entity.ChunkKey
	Data entity.ChunkData
}

type FlusherConfig struct {
	// BatchSize - при скольких накопленных парах батч пишется в БД принудительно
	BatchSize int

	// FlushTime - максимальное время, которое пара ждёт в батче при слабом потоке данных
	FlushTime time.Duration

	// ShutdownFlushTimeout - таймаут на дозапись данных при отмене контекста
	ShutdownFlushTimeout time.Duration

	// WriteRetries - сколько раз повторить запись батча при ошибке БД
	WriteRetries int

	// WriteRetryBackoff - базовая пауза между повторами
	WriteRetryBackoff time.Duration
}

// Flusher решает проблему записи данных в базу, базовый паттерн, задаётся BatchSize и FlushTime -> если батч переполнился -
// принудительно опустошается, отгружая все данные в БД, если поток данных маленький, но всё же есть - срабатывает FlushTime,
// который по определённому кулдауну вызывает flush. в зависимости передаётся оба репозитория и логгер, ничего необычного.
type Flusher struct {
	keyRepo  keyWriter
	dataRepo dataWriter
	logger   *zap.Logger
	cfg      FlusherConfig
}

func NewFlusher(keyRepo keyWriter, dataRepo dataWriter, logger *zap.Logger, cfg FlusherConfig) *Flusher {
	return &Flusher{
		keyRepo:  keyRepo,
		dataRepo: dataRepo,
		logger:   logger,
		cfg:      cfg,
	}
}

// Run читает результаты из in до закрытия канала, батчами записывая их в обе БД.
// Возвращает ошибку первой неудачной записи; при штатном завершении (in закрыт)
// дописывает остаток и возвращает nil.
func (f *Flusher) Run(ctx context.Context, in <-chan FlushItem) error {
	// 1. создаётся два батча для двух БД
	keys := make([]entity.ChunkKey, 0, f.batchSize)
	data := make([]entity.ChunkData, 0, f.batchSize)

	// 2. инициализируется таймер с заданным в конфиге параметром
	timer := time.NewTimer(f.cfg.FlushTime)
	defer timer.Stop()

	// flush - функция-переменная, которая проверяет есть ли данные и если есть - отправляет, обрабатывает ошибку и обнуляет батч
	flush := func() error {
		if len(data) == 0 {
			return nil
		}
		if err := f.write(ctx, keys, data); err != nil {
			return err
		}
		keys = keys[:0]
		data = data[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// дозаписывается всё, что уже есть в data батче
			if len(data) > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), f.cfg.ShutdownFlushTimeout)
				err := f.write(flushCtx, keys, data)
				cancel()
				if err != nil {
					f.logger.Error("flush tail on shutdown failed", zap.Int("lost", len(data)), zap.Error(err))
				}
			}
			f.logger.Info("Flusher worker done")
			return ctx.Err()

		case v, ok := <-in:
			if !ok {
				// если канал закрыт - вызываем последний flush и выходим
				return flush()
			}
			// если всё ок - добавляем в батчи полученные данные.
			keys = append(keys, v.Key)
			data = append(data, v.Data)
			if len(data) >= f.cfg.BatchSize {
				// если полученные данные больше, чем батч - освобождаем батч
				if err := flush(); err != nil {
					return err
				}
				resetTimer(timer, f.cfg.FlushTime)
			}

		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
			timer.Reset(f.cfg.FlushTime)
		}
	}
}

// write кладёт пачку в обе базы: сначала данные, затем ключи. Порядок важен из-за инварианта версий
func (f *Flusher) write(ctx context.Context, keys []entity.ChunkKey, data []entity.ChunkData) error {
	if err := f.dataRepo.SaveData(ctx, data); err != nil {
		return fmt.Errorf("flush data batch (%d): %w", len(data), err)
	}
	if err := f.keyRepo.SaveKeys(ctx, keys); err != nil {
		return fmt.Errorf("flush key batch (%d): %w", len(keys), err)
	}
	f.logger.Debug("batch flushed", zap.Int("count", len(data)))
	return nil
}

// хелпер для ресета таймера
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
