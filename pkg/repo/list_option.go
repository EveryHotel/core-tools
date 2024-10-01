package repo

import "github.com/doug-martin/goqu/v9/exp"

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
