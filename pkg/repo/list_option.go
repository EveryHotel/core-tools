package repo

import "github.com/doug-martin/goqu/v9/exp"

// TODO everyHotel
//  Добавились SqlQueryOptions

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

func WithSqlOptions(sqlOptions []SqlQueryOption) ListOption {
	return func(handler *ListOptionHandler) {
		handler.SqlOptions = sqlOptions
	}
}

type SqlQueryOption func(handler *SqlQueryOptionHandler)

func WithDialect(dialect string) SqlQueryOption {
	return func(handler *SqlQueryOptionHandler) {
		handler.Dialect = dialect
	}
}

func WithPrepared(prepared bool) SqlQueryOption {
	return func(handler *SqlQueryOptionHandler) {
		handler.Prepared = prepared
	}
}

func NewSqlQueryOptionHandler() *SqlQueryOptionHandler {
	return &SqlQueryOptionHandler{}
}

type SqlQueryOptionHandler struct {
	Dialect  string
	Prepared bool
}

func NewListOptionHandler() *ListOptionHandler {
	return &ListOptionHandler{}
}

type ListOptionHandler struct {
	SqlOptions []SqlQueryOption
	Limit      int64
	Offset     int64
	Sort       []exp.OrderedExpression
	Relations  []ListOptionRelation
}

type ListOptionRelation struct {
	Alias       string
	Table       string
	Expressions []exp.Expression
	Nullable    bool
}
