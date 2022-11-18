package repo

import (
	"context"
	"log"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"

	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/database"
)

type ListItemId struct {
	Id int64 `db:"id"`
}

type BaseRepo[T any, ID int64 | string] interface {
	BulkUpdate(context.Context, map[string]interface{}, map[string]interface{}) error
	Create(context.Context, T) (ID, error)
	Delete(context.Context, ID) error
	Get(context.Context, ID) (T, error)
	GetOneBy(context.Context, map[string]interface{}) (T, error)
	ForceDelete(context.Context, ID) error
	List(context.Context) ([]T, error)
	ListBy(context.Context, map[string]interface{}, ...ListOption) ([]T, error)
	SoftDelete(context.Context, ID) error
	Update(context.Context, T) error
	CreateMultiple(context.Context, []T) ([]ListItemId, error)
	UpdateMultiple(context.Context, []T) error
}

type baseRepo[T any, ID int64 | string] struct {
	db        database.DBService
	tableName string
	alias     string
}

func NewRepository[T any, ID int64 | string](db database.DBService, tableName, alias string) BaseRepo[T, ID] {
	return &baseRepo[T, ID]{
		db:        db,
		tableName: tableName,
		alias:     alias,
	}
}

type ListOption func(handler *ListOptionHandler)

func WithLimit(limit int64) ListOption {
	return func(handler *ListOptionHandler) {
		handler.Limit = limit
	}
}

func WithSort(sort []exp.OrderedExpression) ListOption {
	return func(handler *ListOptionHandler) {
		handler.Sort = sort
	}
}

func NewListOptionHandler() *ListOptionHandler {
	return &ListOptionHandler{}
}

type ListOptionHandler struct {
	Limit int64
	Sort  []exp.OrderedExpression
}

// BulkUpdate обновляет записи в таблице по заданному условию
func (r baseRepo[T, ID]) BulkUpdate(ctx context.Context, updateFields, where map[string]interface{}) error {
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
func (r baseRepo[T, ID]) Create(ctx context.Context, entity T) (ID, error) {
	_, rows := SanitizeRowsForInsert(entity)

	ds := goqu.Insert(r.tableName).
		Returning(goqu.C("id")).
		Rows(rows)

	var id ID

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return id, err
	}

	err = r.db.Insert(ctx, sql, args, &id)
	if err != nil {
		log.Println("msg", "cannot Exec insert country sql", "err", err)
		return id, err
	}

	return id, nil
}

// Update обновляет сущность
func (r baseRepo[T, ID]) Update(ctx context.Context, entity T) error {
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
func (r baseRepo[T, ID]) Get(ctx context.Context, id ID) (T, error) {
	var entity T

	ds := goqu.Select(database.Sanitize(entity, database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
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
func (r baseRepo[T, ID]) GetOneBy(ctx context.Context, conditions map[string]interface{}) (T, error) {
	var entity T

	ds := goqu.Select(database.Sanitize(entity, database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
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
func (r baseRepo[T, ID]) List(ctx context.Context) ([]T, error) {
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
func (r baseRepo[T, ID]) ListBy(ctx context.Context, criteria map[string]interface{}, options ...ListOption) ([]T, error) {
	var res []T

	optHandler := NewListOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Select(database.Sanitize(*new(T), database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias))

	if len(criteria) > 0 {
		ds = ds.Where(goqu.Ex(criteria))
	}

	if len(optHandler.Sort) > 0 {
		ds = ds.Order(optHandler.Sort...)
	}

	if optHandler.Limit > 0 {
		ds = ds.Limit(uint(optHandler.Limit))
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
func (r baseRepo[T, ID]) Delete(ctx context.Context, id ID) error {
	isSoftDeleting := IsSoftDeletingEntity(*new(T))

	if isSoftDeleting {
		return r.SoftDelete(ctx, id)
	}

	return r.ForceDelete(ctx, id)
}

// ForceDelete прямое удаление из базы элемента
func (r baseRepo[T, ID]) ForceDelete(ctx context.Context, id ID) error {
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
func (r baseRepo[T, ID]) SoftDelete(ctx context.Context, id ID) error {
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

//CreateMultiple - создает сразу несколько записей в таблице
func (r baseRepo[T, ID]) CreateMultiple(ctx context.Context, entities []T) ([]ListItemId, error) {
	if len(entities) == 0 {
		return nil, nil
	}

	var records []interface{}
	for _, entity := range entities {
		_, rows := SanitizeRowsForInsert(entity)
		records = append(records, rows)
	}
	ds := goqu.Insert(r.tableName).
		Returning(goqu.C("id")).
		Rows(records...)

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return nil, err
	}

	var res []ListItemId
	err = r.db.InsertMany(ctx, sql, args, &res)
	if err != nil {
		log.Println("msg", "cannot Exec insert hotelPhoto sql", "err", err)
		return nil, err
	}

	return res, nil
}

//UpdateMultiple обновляет несколько сущностей
func (r *baseRepo[T, ID]) UpdateMultiple(ctx context.Context, entities []T) error {
	var records []interface{}
	var columns []string
	for _, entity := range entities {
		id, rows := SanitizeRowsForUpdateMultiple(entity)
		if len(columns) == 0 {
			for k := range rows {
				columns = append(columns, k)
			}
		}

		rows["id"] = id

		records = append(records, rows)

	}
	onConflictUpdate := make(map[string]interface{})
	for _, column := range columns {
		onConflictUpdate[column] = goqu.C(column).Table("excluded")
	}
	if _, ok := onConflictUpdate["updated_at"]; ok {
		onConflictUpdate["updated_at"] = time.Now()
	}

	//т.к. goqu не поддерживает postgresql update from values юзаем insert on conflict update
	ds := goqu.Insert(r.tableName).
		Rows(records...).
		OnConflict(goqu.DoUpdate("id", onConflictUpdate))

	sql, args, err := ds.ToSQL()

	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec update city sql", "err", err)
		return err
	}

	return nil
}
