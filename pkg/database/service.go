package database

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/driftprogramming/pgxpoolmock"
	"github.com/jackc/pgx/v4"
)

const CtxDbTxKey = "db_tx"

type DBService interface {
	Dialect() goqu.DialectWrapper
	Exec(ctx context.Context, query string, args []any) error
	Insert(ctx context.Context, sql string, args []any, dest any) error
	Count(ctx context.Context, sql string, args []any) (int64, error)
	SelectOne(ctx context.Context, sql string, args []any, dest any, relations ...string) error
	Select(ctx context.Context, sql string, args []any, dest any, relations ...string) error
	Begin(ctx context.Context) (context.Context, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	InsertMany(ctx context.Context, sql string, args []any, dest any) error
}

type dbService struct {
	pool pgxpoolmock.PgxPool
}

// NewDBService возвращает новый экзмпляр сервиса БД
func NewDBService(pool pgxpoolmock.PgxPool) DBService {
	return &dbService{
		pool: pool,
	}
}

// Dialect возвращает postgres диалект для goqu
func (s *dbService) Dialect() goqu.DialectWrapper {
	return goqu.Dialect("postgres")
}

// Exec выполняет запрос
func (s *dbService) Exec(ctx context.Context, query string, args []any) (err error) {
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		_, err = tx.Exec(ctx, query, args...)
	} else {
		_, err = s.pool.Exec(ctx, query, args...)
	}

	return err
}

// Insert выполняет insert запрос и возвращает данные в dest
func (s *dbService) Insert(ctx context.Context, sql string, args []any, dest any) (err error) {
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		err = tx.QueryRow(ctx, sql, args...).Scan(dest)
	} else {
		err = s.pool.QueryRow(ctx, sql, args...).Scan(dest)
	}

	return err
}

// InsertMany выполняет insert запрос и возвращает множественные данные в dest
func (s *dbService) InsertMany(ctx context.Context, sql string, args []any, dest any) (err error) {
	var rows pgx.Rows
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		rows, err = tx.Query(ctx, sql, args...)
	} else {
		rows, err = s.pool.Query(ctx, sql, args...)
	}

	if err != nil {
		return err
	}

	if err = scanRows(rows, dest); err != nil {
		return err
	}

	return nil
}

// Select выполняет SELECT запрос и сохраняет результаты в массив структур dest
func (s *dbService) Select(ctx context.Context, sql string, args []any, dest any, relations ...string) (err error) {
	var rows pgx.Rows
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		rows, err = tx.Query(ctx, sql, args...)
	} else {
		rows, err = s.pool.Query(ctx, sql, args...)
	}

	if err != nil {
		return err
	}

	if err = scanRows(rows, dest, relations...); err != nil {
		return err
	}

	return nil
}

// SelectOne выполняет SELECT запрос и сохраняет только первый результат в структуру dest
func (s *dbService) SelectOne(ctx context.Context, sql string, args []any, dest any, relations ...string) (err error) {
	var row pgx.Row
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		row = tx.QueryRow(ctx, sql, args...)
	} else {
		row = s.pool.QueryRow(ctx, sql, args...)
	}

	if err = scanRow(row, dest, relations...); err != nil {
		return err
	}

	return nil
}

// Count выполняет COUNT запрос
func (s *dbService) Count(ctx context.Context, sql string, args []any) (count int64, err error) {
	var row pgx.Row
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		row = tx.QueryRow(ctx, sql, args...)
	} else {
		row = s.pool.QueryRow(ctx, sql, args...)
	}

	if err = row.Scan(&count); err != nil {
		return count, err
	}

	return count, nil
}

// Begin Создает транзакцию и возвращает связанный с нею контекст
func (s *dbService) Begin(ctx context.Context) (context.Context, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ctx, err
	}

	ctx = context.WithValue(ctx, CtxDbTxKey, tx)

	return ctx, nil
}

// Commit Применяет транзакцию
func (s *dbService) Commit(ctx context.Context) error {
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
	}

	return nil
}

// Rollback Откатывает транзакцию
func (s *dbService) Rollback(ctx context.Context) error {
	tx, ok := ctx.Value(CtxDbTxKey).(pgx.Tx)
	if ok {
		return tx.Rollback(ctx)
	}

	return nil
}

func scanRows(rows pgx.Rows, dest any, relations ...string) error {
	vDest := reflect.ValueOf(dest).Elem()

	// определяем - является ли сущность с nullable сущностями для оптимизации обычных запросов
	nullable := DestHasNullableRelations(reflect.New(reflect.TypeOf(dest).Elem().Elem()).Elem(), relations...)

	var nullableRow, scanFields []any

	for rows.Next() {
		// создаем nullableRow, куда будет происходить скан
		if nullable {
			nullableRow = GetNullableRowFromOrigDest(reflect.New(reflect.TypeOf(dest).Elem().Elem()).Elem(), false, relations...)
			scanFields = SetNullableDestFields(reflect.ValueOf(nullableRow), []any{})
		}
		// reflect.TypeOf(dest) это указатель
		// первый Elem() разыименовывает его, второй Elem() получает типо элемента в slice
		destItem := reflect.New(reflect.TypeOf(dest).Elem().Elem()).Elem()

		// если это сущность с nullable, то мы сначала сканируем,(тк мы до этого уже сделали scanFields) и потом проставляем заначения в сущность из nullableRow
		if nullable {
			if err := rows.Scan(scanFields...); err != nil {
				return err
			}
			SetDestFromNullable(destItem, nullableRow, false, 0, relations...)
		} else {
			scanFields = setDestFields(destItem, []any{}, relations...)
			if err := rows.Scan(scanFields...); err != nil {
				return err
			}
		}

		vDest.Set(reflect.Append(vDest, destItem))
	}

	return rows.Err()
}

func scanRow(row pgx.Row, dest any, relations ...string) error {
	vDest := reflect.ValueOf(dest).Elem()

	// определяем - является ли сущность с nullable сущностями для оптимизации обычных запросов
	nullable := DestHasNullableRelations(vDest, relations...)

	if nullable {
		// создаем nullableRow, куда будет происходить скан
		nullableRow := GetNullableRowFromOrigDest(vDest, false, relations...)

		scanFields := SetNullableDestFields(reflect.ValueOf(nullableRow), []any{})
		if err := row.Scan(scanFields...); err != nil {
			return err
		}

		//проставляем заначения в сущность из nullableRow
		SetDestFromNullable(vDest, nullableRow, false, 0, relations...)
	} else {
		scanFields := setDestFields(vDest, []any{}, relations...)
		if err := row.Scan(scanFields...); err != nil {
			return err
		}
	}

	return nil
}

func setDestFields(vDest reflect.Value, scanFields []any, relations ...string) []any {
	if vDest.Type().Kind() != reflect.Struct || vDest.Type().Implements(reflect.TypeOf((*sql.Scanner)(nil)).Elem()) {
		scanFields = append(scanFields, vDest.Addr().Interface())
		return scanFields
	}

	for i := 0; i < vDest.NumField(); i++ {
		field := vDest.Field(i)
		typeField := vDest.Type().Field(i)
		tag := typeField.Tag

		if field.Kind() == reflect.Struct && len(relations) > 0 {
			for _, relation := range relations {
				values := strings.Split(tag.Get("relation"), ",")
				if len(values) > 0 && values[0] == relation {
					scanFields = setDestFields(field, scanFields)
				}
			}
		}

		if tag.Get("db") != "" {
			scanFields = append(scanFields, field.Addr().Interface())
		}
		if tag.Get("embedded_struct") == "1" && field.Kind() == reflect.Struct {
			scanFields = setDestFields(field, scanFields)
		}
	}

	return scanFields
}
