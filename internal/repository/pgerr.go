package repository

import (
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// isUniqueViolation - дополнительная проверка на конкретный статус ошибки (дубликат). из-за того, что вставка выполняется
// не через insert into, а через CopyFrom, то нельзя явно обработать ошибку через errors.Is(...), так что этот костыль будет
// использоваться для вставки в keys и data repository.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}
