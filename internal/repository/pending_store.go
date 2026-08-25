package repository

import (
	"context"
	"errors"
	cfg "file-cipher-core/internal/config"
	"file-cipher-core/internal/entity"
	"file-cipher-core/pkg/logger"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// SUMMARY:
// methods:
// 1. AddBatch (+flush)
// 2. Read
// 3. Ack
// 4. Init
// 5. Claim

type PendingStore struct {
	rdb    *redis.Client
	logger logger.Logger
	queue  string // очередь (список) данных, которые ждут обработки

	// курсор, который двигается по списку записей: 0, 1, 2, 3, *cursor*, ..., 8, 9, 10,
	//где всё что до курсора - успешно обработано, а после - не тронуто
	cursorGroup string
	readBlock   time.Duration
	consumer    string
}

func NewPendingStore(rdb *redis.Client, logger logger.Logger, cfg cfg.Redis) (*PendingStore, error) {
	// каждый инстанс приложения имеет уникальный Hostname
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("resolve consumer name: %w", err)
	}
	return &PendingStore{
		rdb:         rdb,
		logger:      logger,
		queue:       cfg.Queue,
		cursorGroup: cfg.CursorName,
		readBlock:   cfg.ReadBlock,
		consumer:    host,
	}, nil
}

// AddBatch - добавить в список батч для последующей записи в Postgres
func (p *PendingStore) AddBatch(ctx context.Context, items []entity.FlushItem) ([]string, error) {
	// за один раундтрип с помощью батча записываем всё в редис
	pipe := p.rdb.Pipeline()

	// cmds создаётся через make, потому что длина известна заранее
	cmds := make([]*redis.StringCmd, 0, len(items))

	// через пайплайн добавляем через XAdd все уникальные значения из items
	for _, item := range items {
		// возвращается указатель на StringCmd, который мы коллектим в cmds
		cmd := pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: p.queue, // имя из конфига
			Values: map[string]any{
				"uuid":       item.Data.UUID.String(),
				"file_id":    item.Data.FileID.String(),
				"ciphertext": item.Data.Ciphertext,
				"nonce":      item.Data.Nonce,
				"key":        item.Key.Key,
				"version":    item.Data.Version,
				"created_at": item.Data.CreatedAt.UnixNano(),
			},
		})
		// собираем все указатели в слайс
		cmds = append(cmds, cmd)
	}

	// вызываем вставку, в результате которой StringCmd заполняются и в cmds указатели уже не указывают на пустышки, а на полностью заполненные структуры
	if _, err := pipe.Exec(ctx); err != nil {
		p.logger.Error("add pending batch failed", "error", err)
		return nil, fmt.Errorf("add pending batch (%d): %w", len(items), err)
	}

	// создаём результирующий []string, куда записываем value из указателей на cmd
	ids := make([]string, 0, len(items))
	for _, cmd := range cmds {
		ids = append(ids, cmd.Val())
	}

	// возвращаем полученные айдишники
	return ids, nil
}

// Read - прочитать необработанные записи батчем
func (p *PendingStore) Read(ctx context.Context, amount int) ([]entity.PendingItem, error) {
	res, err := p.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    p.cursorGroup,
		Consumer: p.consumer,
		Streams:  []string{p.queue, ">"}, // айди, с которого начинаем читать и спец. символ "всё, что группа никому не выдавала"
		Count:    int64(amount),
		Block:    p.readBlock,
		NoAck:    false,
	}).Result()

	// явная проверка на случай, если вся очередь обработана, чтобы сделать фолбек на n секунд (93 строка)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		p.logger.Error("read pending failed", "error", err)
		return nil, fmt.Errorf("read pending (%d): %w", amount, err)
	}

	// сообщения первого полученного стрима
	msgs := res[0].Messages
	items := make([]entity.PendingItem, 0, len(msgs))
	for _, msg := range msgs {
		item, err := parsePendingItem(msg)
		if err != nil {
			p.logger.Error("parse pending item failed", "id", msg.ID, "error", err)
			continue
		}
		items = append(items, item)
	}

	return items, nil
}

// Ack - подтверждение о том, что данные обработаны (записаны в Postgres) и их можно удалять
func (p *PendingStore) Ack(ctx context.Context, ids []string) error {
	if err := p.rdb.XAck(ctx, p.queue, p.cursorGroup, ids...).Err(); err != nil {
		p.logger.Error("ack pending failed", "error", err)
		return fmt.Errorf("ack pending(%d): %w", len(ids), err)
	}
	return nil
}

// CursorInit - инициализация структуры, в которой будет происходить чтение/запись/ack
func (p *PendingStore) CursorInit(ctx context.Context) error {
	err := p.rdb.XGroupCreateMkStream(ctx, p.queue, p.cursorGroup, "0").Err()

	// если приложение рестартится - на стороне редиса нет никакой идемпотентной проверки на группу, так что здесь явно
	// отлавливается ошибка дубликата и пропускается. в ином случае есть шанс, что при запуске нескольких инстансов одновременно
	// случится гонка, а явно проверять через XInfoGroups и XGroupCreat - дорого из-за двух раундтрипов
	if err != nil && !redis.HasErrorPrefix(err, "BUSYGROUP") {
		p.logger.Error("cursor init failed", "error", err)
		return fmt.Errorf("cursor init: %w", err)
	}
	return nil
}

// Claim - "забрать все недописанные записи после рестарта приложения"
func (p *PendingStore) Claim(ctx context.Context, minIdle time.Duration, amount int) ([]entity.PendingItem, error) {
	msgs, _, err := p.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   p.queue,
		Group:    p.cursorGroup,
		Consumer: p.consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    int64(amount),
	}).Result()
	if err != nil {
		p.logger.Error("claim pending failed", "error", err)
		return nil, fmt.Errorf("claim pending: %w", err)
	}

	items := make([]entity.PendingItem, 0, len(msgs))
	for _, msg := range msgs {
		item, parseErr := parsePendingItem(msg)
		if parseErr != nil {
			p.logger.Error("parse claimed item failed", "id", msg.ID, "error", parseErr)
			continue
		}
		items = append(items, item)
	}

	return items, nil
}

// parsePendingItem - хелпер, который парсит XMessage в entity PendingItem
func parsePendingItem(msg redis.XMessage) (entity.PendingItem, error) {
	dataUUID, err := parseUUID(msg.Values, "uuid")
	if err != nil {
		return entity.PendingItem{}, err
	}
	fileID, err := parseUUID(msg.Values, "file_id")
	if err != nil {
		return entity.PendingItem{}, err
	}
	ciphertext, err := parseString(msg.Values, "ciphertext")
	if err != nil {
		return entity.PendingItem{}, err
	}
	nonce, err := parseString(msg.Values, "nonce")
	if err != nil {
		return entity.PendingItem{}, err
	}
	key, err := parseString(msg.Values, "key")
	if err != nil {
		return entity.PendingItem{}, err
	}
	version, err := parseInt(msg.Values, "version")
	if err != nil {
		return entity.PendingItem{}, err
	}
	createdAtNano, err := parseInt64(msg.Values, "created_at")
	if err != nil {
		return entity.PendingItem{}, err
	}
	createdAt := time.Unix(0, createdAtNano)

	return entity.PendingItem{
		ID: msg.ID,
		Item: entity.FlushItem{
			Key: entity.ChunkKey{
				UUID:      dataUUID,
				Key:       []byte(key),
				Version:   version,
				CreatedAt: createdAt,
			},
			Data: entity.ChunkData{
				UUID:       dataUUID,
				FileID:     fileID,
				Ciphertext: []byte(ciphertext),
				Nonce:      []byte(nonce),
				Version:    version,
				CreatedAt:  createdAt,
			},
		},
	}, nil
}

// хелперы для парсинга string, uuid, int, int64,

func parseString(values map[string]any, field string) (string, error) {
	v, ok := values[field].(string)
	if !ok {
		return "", fmt.Errorf("field %q: expected string, got %T", field, values[field])
	}
	return v, nil
}

func parseUUID(values map[string]any, field string) (uuid.UUID, error) {
	v, err := parseString(values, field)
	if err != nil {
		return uuid.UUID{}, err
	}
	id, err := uuid.Parse(v)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("field %q: parse uuid: %w", field, err)
	}
	return id, nil
}

func parseInt(values map[string]any, field string) (int, error) {
	v, err := parseString(values, field)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("field %q: parse int: %w", field, err)
	}
	return n, nil
}

func parseInt64(values map[string]any, field string) (int64, error) {
	v, err := parseString(values, field)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q: parse int64: %w", field, err)
	}
	return n, nil
}
