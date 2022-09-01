package repo

import (
	"context"
	"log"
	"time"

	"github.com/doug-martin/goqu/v9"

	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/database"
)

type BaseRepo[T any] interface {
	BulkUpdate(context.Context, map[string]interface{}, map[string]interface{}) error
	Create(context.Context, T) (int64, error)
	Delete(context.Context, int64) error
	Get(context.Context, int64) (T, error)
	GetOneBy(context.Context, map[string]interface{}) (T, error)
	ForceDelete(context.Context, int64) error
	List(context.Context) ([]T, error)
	ListBy(context.Context, map[string]interface{}) ([]T, error)
	SoftDelete(context.Context, int64) error
	Update(context.Context, T) error
}

type baseRepo[T any] struct {
	db        database.DBService
	tableName string
	alias     string
}

func NewRepository[T any](db database.DBService, tableName, alias string) BaseRepo[T] {
	return &baseRepo[T]{
		db:        db,
		tableName: tableName,
		alias:     alias,
	}
}

// BulkUpdate обновляет записи в таблице по заданному условию
func (r baseRepo[T]) BulkUpdate(ctx context.Context, updateFields, where map[string]interface{}) error {
	ds := goqu.Update(r.tableName).
		Where(goqu.Ex(where)).
		Set(goqu.Record(updateFields))

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec bulk update city sql", "err", err)
		return err
	}

	return nil
}

// Create создает новую сущность
func (r baseRepo[T]) Create(ctx context.Context, entity T) (int64, error) {
	_, rows := SanitizeRowsForInsert(entity)

	ds := goqu.Insert(r.tableName).
		Returning(goqu.C("id")).
		Rows(rows)

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return 0, err
	}

	var id int64
	err = r.db.Insert(ctx, sql, args, &id)
	if err != nil {
		log.Println("msg", "cannot Exec insert country sql", "err", err)
		return 0, err
	}

	return id, nil
}

// Update обновляет сущность
func (r baseRepo[T]) Update(ctx context.Context, entity T) error {
	id, rows := SanitizeRowsForUpdate(entity)

	ds := goqu.Update(r.tableName).
		Where(goqu.C("id").Eq(id)).
		Set(rows)

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec update country sql", "err", err)
		return err
	}

	return nil
}

// Get возвращает сущность по id
func (r baseRepo[T]) Get(ctx context.Context, id int64) (T, error) {
	var entity T

	ds := goqu.Select(database.Sanitize(entity)...).
		From(r.tableName).
		Where(goqu.Ex{
			"id": id,
		})

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "sql", sql, "err", err)
		return entity, err
	}

	if err = r.db.SelectOne(ctx, sql, args, &entity); err != nil {
		return entity, err
	}

	return entity, nil
}

// GetOneBy возвращает сущность по указанным параметрам
func (r baseRepo[T]) GetOneBy(ctx context.Context, conditions map[string]interface{}) (T, error) {
	var entity T

	ds := goqu.Select(database.Sanitize(entity)...).
		From(r.tableName).
		Where(goqu.Ex(conditions))

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "sql", sql, "err", err)
		return entity, err
	}

	if err = r.db.SelectOne(ctx, sql, args, &entity); err != nil {
		return entity, err
	}

	return entity, nil
}

// List возвращает список сущностей
func (r baseRepo[T]) List(ctx context.Context) ([]T, error) {
	var res []T

	ds := goqu.Select(database.Sanitize(*new(T), database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias))

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "sql", sql, "err", err)
		return res, err
	}

	if err = r.db.Select(ctx, sql, args, &res); err != nil {
		return res, err
	}

	return res, nil
}

// ListBy возвращает список сущностей по критерию
func (r baseRepo[T]) ListBy(ctx context.Context, criteria map[string]interface{}) ([]T, error) {
	var res []T

	ds := goqu.Select(database.Sanitize(*new(T), database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias))

	if len(criteria) > 0 {
		ds = ds.Where(goqu.Ex(criteria))
	}

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "sql", sql, "err", err)
		return res, err
	}

	if err = r.db.Select(ctx, sql, args, &res); err != nil {
		return res, err
	}

	return res, nil
}

// Delete удаление записи из таблицы
func (r baseRepo[T]) Delete(ctx context.Context, id int64) error {
	isSoftDeleting := IsSoftDeletingEntity(*new(T))

	if isSoftDeleting {
		return r.SoftDelete(ctx, id)
	}

	return r.ForceDelete(ctx, id)
}

// ForceDelete прямое удаление из базы элемента
func (r baseRepo[T]) ForceDelete(ctx context.Context, id int64) error {
	ds := goqu.Delete(r.tableName).
		Where(goqu.C("id").Eq(id))

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build deleting SQL query", "err", err)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec delete sql", "err", err)
		return err
	}

	return nil
}

// SoftDelete помечает сущность, как удаленную
func (r baseRepo[T]) SoftDelete(ctx context.Context, id int64) error {
	ds := goqu.Update(r.tableName).
		Where(goqu.C("id").Eq(id)).
		Set(goqu.Record{
			"deleted_at": time.Now(),
		})

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build soft deleting SQL query", "err", err)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec delete sql", "err", err)
		return err
	}

	return nil
}
