package repo

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/jackc/pgx/v4"

	"github.com/EveryHotel/core-tools/pkg/database"
)

// TODO everyHotel
//  Здесь было много всего в основном из-за SqlQueryOption
//  Они у нас используются при сохранении и обновлении логов, там надо обязательно указывать WithPrepared и WithDialect

type BaseRepo[T any, ID int64 | string] interface {
	BulkUpdate(context.Context, map[string]any, map[string]any, ...SqlQueryOption) error
	Create(context.Context, T, ...SqlQueryOption) (ID, error)
	Delete(context.Context, ID, ...SqlQueryOption) error
	DeleteBy(context.Context, map[string]any, ...SqlQueryOption) error
	Get(context.Context, ID, ...ListOptionRelation) (T, error)
	GetOneBy(context.Context, map[string]any, ...ListOptionRelation) (T, error)
	ForceDelete(context.Context, ID, ...SqlQueryOption) error
	ForceDeleteBy(context.Context, map[string]any, ...SqlQueryOption) error
	List(context.Context, ...SqlQueryOption) ([]T, error)
	ListBy(context.Context, map[string]any, ...ListOption) ([]T, error)
	ListByExpression(context.Context, exp.ExpressionList, ...ListOption) ([]T, error)
	SoftDelete(context.Context, ID, ...SqlQueryOption) error
	SoftDeleteMultiple(context.Context, []ID) error
	Update(context.Context, T, ...SqlQueryOption) error
	CreateMultiple(context.Context, []T, ...SqlQueryOption) ([]ID, error)
	UpdateMultiple(context.Context, []T, ...SqlQueryOption) error
	ForceDeleteMultiple(context.Context, []ID) error
	DeleteAndMoveReferences(ctx context.Context, id ID, newId ID) error
}

type baseRepo[T any, ID int64 | string] struct {
	db        database.DBService
	tableName string
	alias     string
	idColumn  string
}

func NewRepository[T any, ID int64 | string](db database.DBService, tableName, alias, idColumn string) BaseRepo[T, ID] {
	if idColumn == "" {
		idColumn = alias + ".id"
	}
	return &baseRepo[T, ID]{
		db:        db,
		tableName: tableName,
		alias:     alias,
		idColumn:  idColumn,
	}
}

// BulkUpdate обновляет записи в таблице по заданному условию
func (r baseRepo[T, ID]) BulkUpdate(ctx context.Context, updateFields, where map[string]any, options ...SqlQueryOption) error {
	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Dialect(optHandler.Dialect).
		Update(r.tableName).
		Set(goqu.Record(updateFields))

	// возможность обновлять всю таблицу без условий
	if where != nil {
		ds = ds.Where(goqu.Ex(where))
	}

	ds = ds.Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for bulk update",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec query for bulk update",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("update", updateFields),
			slog.Any("where", where),
		)
		return err
	}

	return nil
}

// Create создает новую сущность
func (r baseRepo[T, ID]) Create(ctx context.Context, entity T, options ...SqlQueryOption) (ID, error) {
	_, rows := SanitizeRowsForInsert[ID](entity)

	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Dialect(optHandler.Dialect).Insert(r.tableName).
		Returning(goqu.C(r.idColumn)).
		Rows(rows).Prepared(true).
		Prepared(optHandler.Prepared)

	var id ID

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for insert",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return id, err
	}

	err = r.db.Insert(ctx, sql, args, &id)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec insert",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("rows", rows),
		)
		return id, err
	}

	return id, nil
}

// Update обновляет сущность
func (r baseRepo[T, ID]) Update(ctx context.Context, entity T, options ...SqlQueryOption) error {
	id, rows := SanitizeRowsForUpdate[ID](entity)

	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Dialect(optHandler.Dialect).Update(r.tableName).
		Where(goqu.C(r.idColumn).Eq(id)).
		Set(rows).
		Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for update",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("id", id),
			slog.String("sql", sql),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec update",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("id", id),
			slog.Any("data", rows),
		)
		return err
	}

	return nil
}

// Get возвращает сущность по id
func (r *baseRepo[T, ID]) Get(ctx context.Context, id ID, relations ...ListOptionRelation) (T, error) {
	idCondKey := r.idColumn
	// если в idColumn только наименование колонки, то добавляем префикс
	if !strings.Contains(idCondKey, ".") {
		idCondKey = r.alias + "." + idCondKey
	}
	return r.GetOneBy(ctx, map[string]any{
		idCondKey: id,
	}, relations...)
}

// GetOneBy возвращает сущность по указанным параметрам
func (r *baseRepo[T, ID]) GetOneBy(ctx context.Context, conditions map[string]any, relations ...ListOptionRelation) (T, error) {
	var entity T

	var relationAliases []string
	for _, relation := range relations {
		relationAliases = append(relationAliases, relation.Alias)
	}

	ds := goqu.Select(database.Sanitize(entity, database.WithPrefix(r.alias), database.WithRelations(relationAliases...))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
		Where(goqu.Ex(conditions))

	ds = applyRelations(ds, relations)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return entity, err
	}

	if err = r.db.SelectOne(ctx, sql, args, &entity, relationAliases...); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.ErrorContext(ctx, "Error during exec select",
				slog.Any("error", err),
				slog.String("table", r.tableName),
				slog.Any("conditions", conditions),
			)
		}
		return entity, err
	}

	return entity, nil
}

// List возвращает список сущностей
func (r baseRepo[T, ID]) List(ctx context.Context, options ...SqlQueryOption) ([]T, error) {
	var res []T

	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Dialect(optHandler.Dialect).
		Select(database.Sanitize(*new(T), database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
		Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return res, err
	}

	if err = r.db.Select(ctx, sql, args, &res); err != nil {
		slog.ErrorContext(ctx, "Error during exec select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
		)
		return res, err
	}

	return res, nil
}

// ListBy возвращает список сущностей по критерию
func (r *baseRepo[T, ID]) ListBy(ctx context.Context, criteria map[string]any, options ...ListOption) ([]T, error) {
	expression := goqu.And(goqu.Ex(criteria))
	return r.ListByExpression(ctx, expression, options...)
}

// ListByExpression возвращает список сущностей по выражению
func (r *baseRepo[T, ID]) ListByExpression(ctx context.Context, criteria exp.ExpressionList, options ...ListOption) ([]T, error) {
	var res []T

	optHandler := NewListOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	sqlOptHandler := NewSqlQueryOptionHandler()
	for _, opt := range optHandler.SqlOptions {
		opt(sqlOptHandler)
	}

	var relations []string
	if len(optHandler.Relations) > 0 {
		for _, r := range optHandler.Relations {
			relations = append(relations, r.Alias)
		}
	}

	ds := goqu.Dialect(sqlOptHandler.Dialect).Select(database.Sanitize(*new(T), database.WithPrefix(r.alias), database.WithRelations(relations...))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
		Prepared(sqlOptHandler.Prepared)

	ds = applyRelations(ds, optHandler.Relations)

	if !criteria.IsEmpty() {
		ds = ds.Where(criteria)
	}

	if len(optHandler.Sort) > 0 {
		ds = ds.Order(optHandler.Sort...)
	}

	if optHandler.Limit > 0 {
		ds = ds.Limit(uint(optHandler.Limit))
	}

	if optHandler.Offset > 0 {
		ds = ds.Offset(uint(optHandler.Offset))
	}

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return res, err
	}

	if err = r.db.Select(ctx, sql, args, &res, relations...); err != nil {
		slog.ErrorContext(ctx, "Error during exec select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("criteria", criteria),
		)
		return res, err
	}

	return res, nil
}

// Delete удаление записи из таблицы
func (r baseRepo[T, ID]) Delete(ctx context.Context, id ID, options ...SqlQueryOption) error {
	isSoftDeleting := IsSoftDeletingEntity(*new(T))

	if isSoftDeleting {
		return r.SoftDelete(ctx, id, options...)
	}

	return r.ForceDelete(ctx, id, options...)
}

// ForceDelete прямое удаление из базы элемента
func (r baseRepo[T, ID]) ForceDelete(ctx context.Context, id ID, options ...SqlQueryOption) error {

	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}
	ds := goqu.Dialect(optHandler.Dialect).Delete(r.tableName).
		Where(goqu.C(r.idColumn).Eq(id)).
		Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)

		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("id", id),
		)
		return err
	}

	return nil
}

// SoftDelete помечает сущность, как удаленную
func (r baseRepo[T, ID]) SoftDelete(ctx context.Context, id ID, options ...SqlQueryOption) error {
	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Dialect(optHandler.Dialect).Update(r.tableName).
		Where(goqu.C(r.idColumn).Eq(id)).
		Set(goqu.Record{
			"deleted_at": time.Now(),
		}).
		Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for soft delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
			slog.Any("id", id),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec soft delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("id", id),
		)
		return err
	}

	return nil
}

// SoftDeleteMultiple помечает пачку сущностей, как удаленные
func (r *baseRepo[T, ID]) SoftDeleteMultiple(ctx context.Context, ids []ID) error {
	ds := goqu.Update(r.tableName).
		Where(goqu.C(r.idColumn).In(ids)).
		Set(goqu.Record{
			"deleted_at": time.Now(),
		})

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for multiple soft delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
			slog.Any("ids", ids),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec multiple soft delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("ids", ids),
		)
		return err
	}

	return nil
}

// DeleteBy удаление записей из таблицы по заданному критерию
func (r baseRepo[T, ID]) DeleteBy(ctx context.Context, criteria map[string]any, options ...SqlQueryOption) error {
	isSoftDeleting := IsSoftDeletingEntity(*new(T))

	if isSoftDeleting {
		return r.BulkUpdate(ctx, map[string]any{
			"deleted_at": time.Now(),
		}, criteria, options...)
	}

	return r.ForceDeleteBy(ctx, criteria, options...)
}

// ForceDeleteBy прямое удаление из базы записей по заданному критерию
func (r baseRepo[T, ID]) ForceDeleteBy(ctx context.Context, criteria map[string]any, options ...SqlQueryOption) error {
	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	ds := goqu.Dialect(optHandler.Dialect).Delete(r.tableName).
		Prepared(optHandler.Prepared)

	if len(criteria) > 0 {
		ds = ds.Where(goqu.Ex(criteria))
	}

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for force delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec force delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
		)
		return err
	}

	return nil
}

// CreateMultiple - создает сразу несколько записей в таблице
func (r baseRepo[T, ID]) CreateMultiple(ctx context.Context, entities []T, options ...SqlQueryOption) ([]ID, error) {
	if len(entities) == 0 {
		return nil, nil
	}

	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	var records []any
	for _, entity := range entities {
		_, rows := SanitizeRowsForInsert[ID](entity)
		records = append(records, rows)
	}
	ds := goqu.Dialect(optHandler.Dialect).Insert(r.tableName).
		Returning(goqu.C(r.idColumn)).
		Rows(records...).
		Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for multiple insert",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return nil, err
	}

	var res []ID
	err = r.db.InsertMany(ctx, sql, args, &res)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec multiple insert",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("values", records),
		)
		return nil, err
	}

	return res, nil
}

// UpdateMultiple обновляет несколько сущностей
func (r *baseRepo[T, ID]) UpdateMultiple(ctx context.Context, entities []T, options ...SqlQueryOption) error {
	if len(entities) == 0 {
		return nil
	}

	var records []any

	optHandler := NewSqlQueryOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	conflictTarget, updateFields := BuildConflictUpdate(entities[0])

	for _, entity := range entities {
		id, rows := SanitizeRowsForInsert[ID](entity)

		// оставляем ID колонку только в случае, когда она является целью конфликта
		if conflictTarget == r.idColumn {
			rows[r.idColumn] = id
		}

		records = append(records, rows)
	}

	//т.к. goqu не поддерживает postgresql update from values юзаем insert on conflict update
	ds := goqu.Dialect(optHandler.Dialect).Insert(r.tableName).
		Rows(records...).
		OnConflict(goqu.DoUpdate(conflictTarget, updateFields)).
		Prepared(optHandler.Prepared)

	sql, args, err := ds.ToSQL()

	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for multiple update",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec multiple update",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("values", records),
		)
		return err
	}

	return nil
}

// ForceDeleteMultiple прямое удаление множества сущностей по ids
func (r *baseRepo[T, ID]) ForceDeleteMultiple(ctx context.Context, ids []ID) error {
	ds := goqu.Delete(r.tableName).
		Where(goqu.C(r.idColumn).In(ids))

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for multiple delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return err
	}

	err = r.db.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec multiple delete",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("ids", ids),
		)
		return err
	}

	return nil
}

// DeleteAndMoveReferences необходимо переопределить в репозитории, если необходимо перемещать ссылки
func (r *baseRepo[T, ID]) DeleteAndMoveReferences(ctx context.Context, id ID, newId ID) error {
	return r.Delete(ctx, id)
}

func applyRelations(ds *goqu.SelectDataset, relations []ListOptionRelation) *goqu.SelectDataset {
	for _, r := range relations {
		if r.Nullable {
			ds = ds.LeftJoin(database.GetTableName(r.Table).As(r.Alias), goqu.On(r.Expressions...))
		} else {
			ds = ds.InnerJoin(database.GetTableName(r.Table).As(r.Alias), goqu.On(r.Expressions...))
		}
	}

	return ds
}
