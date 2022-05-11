package database

import (
	"context"
	"reflect"

	"github.com/doug-martin/goqu/v9"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type DBService interface {
	Dialect() goqu.DialectWrapper
	Exec(ctx context.Context, query string, args []interface{}) error
	Insert(ctx context.Context, sql string, args []interface{}, dest interface{}) error
	Count(ctx context.Context, sql string, args []interface{}) (int64, error)
	SelectOne(ctx context.Context, sql string, args []interface{}, dest interface{}) error
	Select(ctx context.Context, sql string, args []interface{}, dest interface{}) error
	Begin(ctx context.Context) (pgx.Tx, error)
	InsertMany(ctx context.Context, sql string, args []interface{}, dest interface{}) error
}

type dbService struct {
	pool *pgxpool.Pool
}

// NewDBService возвращает новый экзмпляр сервиса БД
func NewDBService(pool *pgxpool.Pool) DBService {
	return &dbService{
		pool: pool,
	}
}

// Dialect возвращает postgres диалект для goqu
func (s *dbService) Dialect() goqu.DialectWrapper {
	return goqu.Dialect("postgres")
}

// Exec выполняет запрос
func (s *dbService) Exec(ctx context.Context, query string, args []interface{}) error {
	_, err := s.pool.Exec(ctx, query, args...)

	return err
}

// Insert выполняет insert запрос и возвращает данные в dest
func (s *dbService) Insert(ctx context.Context, sql string, args []interface{}, dest interface{}) error {
	err := s.pool.QueryRow(ctx, sql, args...).Scan(dest)

	return err
}

// InsertMany выполняет insert запрос и возвращает множественные данные в dest
func (s *dbService) InsertMany(ctx context.Context, sql string, args []interface{}, dest interface{}) error {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}

	if err = scanRows(rows, dest); err != nil {
		return err
	}

	return nil
}

// Select выполняет SELECT запрос и сохраняет результаты в массив структур dest
func (s *dbService) Select(ctx context.Context, sql string, args []interface{}, dest interface{}) error {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}

	if err = scanRows(rows, dest); err != nil {
		return err
	}

	return nil
}

// SelectOne выполняет SELECT запрос и сохраняет только первый результат в структуру dest
func (s *dbService) SelectOne(ctx context.Context, sql string, args []interface{}, dest interface{}) error {
	row := s.pool.QueryRow(ctx, sql, args...)

	if err := scanRow(row, dest); err != nil {
		return err
	}

	return nil
}

// Count выполняет COUNT запрос
func (s *dbService) Count(ctx context.Context, sql string, args []interface{}) (int64, error) {
	row := s.pool.QueryRow(ctx, sql, args...)

	var count int64
	if err := row.Scan(&count); err != nil {
		return count, err
	}

	return count, nil
}

// Begin Создает транзакцию
func (s *dbService) Begin(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

func scanRows(rows pgx.Rows, dest interface{}) error {
	columns := make([]string, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		columns[i] = string(field.Name)
	}

	vDest := reflect.ValueOf(dest).Elem()

	for rows.Next() {
		// reflect.TypeOf(dest) это указатель
		// первый Elem() разыименовывает его, второй Elem() получает типо элемента в slice
		destItem := reflect.New(reflect.TypeOf(dest).Elem().Elem()).Elem()
		var scanFields []interface{}
		for i := 0; i < destItem.NumField(); i++ {
			field := destItem.Field(i)
			typeField := destItem.Type().Field(i)
			tag := typeField.Tag

			if tag.Get("db") != "" {
				scanFields = append(scanFields, field.Addr().Interface())
			}
		}

		if err := rows.Scan(scanFields...); err != nil {
			return err
		}

		vDest.Set(reflect.Append(vDest, destItem))
	}

	return rows.Err()
}

func scanRow(row pgx.Row, dest interface{}) error {
	vDest := reflect.ValueOf(dest).Elem()

	var scanFields []interface{}
	for i := 0; i < vDest.NumField(); i++ {
		field := vDest.Field(i)
		typeField := vDest.Type().Field(i)
		tag := typeField.Tag

		if tag.Get("db") != "" {
			scanFields = append(scanFields, field.Addr().Interface())
		}
	}

	if err := row.Scan(scanFields...); err != nil {
		return err
	}

	return nil
}
