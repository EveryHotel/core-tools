package repo

import (
	"context"
	"log/slog"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/EveryHotel/core-tools/pkg/database"
)

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
	CreateMultiple(context.Context, []T) ([]ID, error)
	UpdateMultiple(context.Context, []T) error
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
		idColumn = "id"
	}
	return &baseRepo[T, ID]{
		db:        db,
		tableName: tableName,
		alias:     alias,
		idColumn:  idColumn,
	}
}

type ListOption func(handler *ListOptionHandler)

func WithLimit(limit int64) ListOption {
	return func(handler *ListOptionHandler) {
		handler.Limit = limit
	}
}

func WithOffset(offset int64) ListOption {
	return func(handler *ListOptionHandler) {
		handler.Offset = offset
	}
}

func WithSort(sort []exp.OrderedExpression) ListOption {
	return func(handler *ListOptionHandler) {
		handler.Sort = sort
	}
}

func WithRelations(relations []ListOptionRelation) ListOption {
	return func(handler *ListOptionHandler) {
		handler.Relations = relations
	}
}

func NewListOptionHandler() *ListOptionHandler {
	return &ListOptionHandler{}
}

type ListOptionHandler struct {
	Limit     int64
	Offset    int64
	Sort      []exp.OrderedExpression
	Relations []ListOptionRelation
}

type ListOptionRelation struct {
	Alias       string
	Table       string
	Expressions []exp.Expression
	Nullable    bool
}

// BulkUpdate обновляет записи в таблице по заданному условию
func (r *baseRepo[T, ID]) BulkUpdate(ctx context.Context, updateFields, where map[string]interface{}) error {
	ds := goqu.Update(r.tableName).
		Where(goqu.Ex(where)).
		Set(goqu.Record(updateFields))

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
func (r *baseRepo[T, ID]) Create(ctx context.Context, entity T) (ID, error) {
	_, rows := SanitizeRowsForInsert[ID](entity)

	ds := goqu.Insert(r.tableName).
		Returning(goqu.C(r.idColumn)).
		Rows(rows)

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
func (r *baseRepo[T, ID]) Update(ctx context.Context, entity T) error {
	id, rows := SanitizeRowsForUpdate[ID](entity)

	ds := goqu.Update(r.tableName).
		Where(goqu.C(r.idColumn).Eq(id)).
		Set(rows)

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
func (r *baseRepo[T, ID]) Get(ctx context.Context, id ID) (T, error) {
	var entity T

	ds := goqu.Select(database.Sanitize(entity, database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
		Where(goqu.Ex{
			r.idColumn: id,
		})

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return entity, err
	}

	if err = r.db.SelectOne(ctx, sql, args, &entity); err != nil {
		slog.ErrorContext(ctx, "Error during exec select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
		)
		return entity, err
	}

	return entity, nil
}

// GetOneBy возвращает сущность по указанным параметрам
func (r *baseRepo[T, ID]) GetOneBy(ctx context.Context, conditions map[string]interface{}) (T, error) {
	var entity T

	ds := goqu.Select(database.Sanitize(entity, database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
		Where(goqu.Ex(conditions))

	sql, args, err := ds.ToSQL()
	if err != nil {
		slog.ErrorContext(ctx, "Cannot build SQL query for select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.String("sql", sql),
		)
		return entity, err
	}

	if err = r.db.SelectOne(ctx, sql, args, &entity); err != nil {
		slog.ErrorContext(ctx, "Error during exec select",
			slog.Any("error", err),
			slog.String("table", r.tableName),
			slog.Any("conditions", conditions),
		)
		return entity, err
	}

	return entity, nil
}

// List возвращает список сущностей
func (r *baseRepo[T, ID]) List(ctx context.Context) ([]T, error) {
	var res []T

	ds := goqu.Select(database.Sanitize(*new(T), database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias))

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
func (r *baseRepo[T, ID]) ListBy(ctx context.Context, criteria map[string]interface{}, options ...ListOption) ([]T, error) {
	var res []T

	optHandler := NewListOptionHandler()
	for _, opt := range options {
		opt(optHandler)
	}

	var relations []string
	if len(optHandler.Relations) > 0 {
		for _, r := range optHandler.Relations {
			relations = append(relations, r.Alias)
		}
	}

	ds := goqu.Select(database.Sanitize(*new(T), database.WithPrefix(r.alias), database.WithRelations(relations...))...).
		From(database.GetTableName(r.tableName).As(r.alias))

	if len(optHandler.Relations) > 0 {
		for _, r := range optHandler.Relations {
			if r.Nullable {
				ds = ds.LeftJoin(database.GetTableName(r.Table).As(r.Alias), goqu.On(r.Expressions...))
			} else {
				ds = ds.InnerJoin(database.GetTableName(r.Table).As(r.Alias), goqu.On(r.Expressions...))
			}
		}
	}

	if len(criteria) > 0 {
		ds = ds.Where(goqu.Ex(criteria))
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
func (r *baseRepo[T, ID]) Delete(ctx context.Context, id ID) error {
	isSoftDeleting := IsSoftDeletingEntity(*new(T))

	if isSoftDeleting {
		return r.SoftDelete(ctx, id)
	}

	return r.ForceDelete(ctx, id)
}

// ForceDelete прямое удаление из базы элемента
func (r *baseRepo[T, ID]) ForceDelete(ctx context.Context, id ID) error {
	ds := goqu.Delete(r.tableName).
		Where(goqu.C(r.idColumn).Eq(id))

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
func (r *baseRepo[T, ID]) SoftDelete(ctx context.Context, id ID) error {
	ds := goqu.Update(r.tableName).
		Where(goqu.C(r.idColumn).Eq(id)).
		Set(goqu.Record{
			"deleted_at": time.Now(),
		})

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

// CreateMultiple - создает сразу несколько записей в таблице
func (r *baseRepo[T, ID]) CreateMultiple(ctx context.Context, entities []T) ([]ID, error) {
	if len(entities) == 0 {
		return nil, nil
	}

	var records []interface{}
	for _, entity := range entities {
		_, rows := SanitizeRowsForInsert[ID](entity)
		records = append(records, rows)
	}
	ds := goqu.Insert(r.tableName).
		Returning(goqu.C(r.idColumn)).
		Rows(records...)

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
func (r *baseRepo[T, ID]) UpdateMultiple(ctx context.Context, entities []T) error {
	var records []interface{}
	var columns []string
	for _, entity := range entities {
		id, rows := SanitizeRowsForUpdateMultiple[ID](entity)
		if len(columns) == 0 {
			for k := range rows {
				columns = append(columns, k)
			}
		}

		rows[r.idColumn] = id

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
		OnConflict(goqu.DoUpdate(r.idColumn, onConflictUpdate))

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
