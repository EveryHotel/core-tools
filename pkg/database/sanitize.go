package database

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jonboulle/clockwork"
)

type SanitizeOption func(*sanitizeOptionHandler)

type sanitizeOptionHandler struct {
	relationDests []any
	relations     []string
	posRelations  []int
	cols          []any
}

// Sanitize возвращает список доступных DB полей у предоставленной структуры
func Sanitize(dest any, opts ...SanitizeOption) []any {
	vDest := reflect.ValueOf(dest)
	var cols []any
	var relationDests []any
	var relations []string
	var posRelations []int

	for i := 0; i < vDest.NumField(); i++ {
		typeField := vDest.Type().Field(i)
		tag := typeField.Tag

		cols = append(cols, fetchColumns(typeField)...)

		if tagVal := tag.Get("relation"); tagVal != "" {
			relationsField := strings.Split(tag.Get("relation"), ",")
			var vDestI any
			kind := vDest.Field(i).Kind()

			if kind == reflect.Pointer {
				//создаем новый интерфейс из типа указателя
				vDestI = reflect.New(vDest.Field(i).Type().Elem()).Elem().Interface()
			} else {
				vDestI = vDest.Field(i).Interface()
			}
			relationDests = append(relationDests, vDestI)
			relations = append(relations, relationsField[0])
			posRelations = append(posRelations, len(cols))
		}
	}

	optHandler := newSanitizeOptionHandler(cols, relationDests, relations, posRelations)
	for _, opt := range opts {
		opt(optHandler)
	}

	return optHandler.GetCols()
}

func fetchColumns(typeField reflect.StructField) []any {
	var cols []any
	if tagVal := typeField.Tag.Get("db"); tagVal != "" {
		cols = append(cols, tagVal)
	}

	embeddedStruct := typeField.Tag.Get("embedded_struct")
	if embeddedStruct == "1" && typeField.Type.Kind() == reflect.Struct {
		for i := 0; i < typeField.Type.NumField(); i++ {
			cols = append(cols, fetchColumns(typeField.Type.Field(i))...)
		}
	}

	return cols
}

func newSanitizeOptionHandler(cols []any, relationDests []any, relations []string, posRelations []int) *sanitizeOptionHandler {
	return &sanitizeOptionHandler{
		relationDests: relationDests,
		relations:     relations,
		posRelations:  posRelations,
		cols:          cols,
	}
}

// WithPrefix возвращает опцию для задания префикса полей
func WithPrefix(p string) SanitizeOption {
	return func(o *sanitizeOptionHandler) {
		o.ApplyPrefix(p)
	}
}

// WithRelations возвращает опцию для задания связей для joins
func WithRelations(r ...string) SanitizeOption {
	return func(o *sanitizeOptionHandler) {
		o.SetRelations(r...)
	}
}

// ApplyPrefix применяет префикс ко всем полям
func (o *sanitizeOptionHandler) ApplyPrefix(p string) {
	for i, col := range o.cols {
		o.cols[i] = fmt.Sprintf("%s.%s", p, col)
	}
}

// SetRelations ищет по префиксу всех связей в структуре
func (o *sanitizeOptionHandler) SetRelations(rels ...string) {
	countShift := 0
	for i, relation := range o.relations {
		for _, rel := range rels {
			if relation == rel {
				relationsCols := Sanitize(o.relationDests[i], WithPrefix(rel))

				positionShift := o.posRelations[i] + countShift
				beforeCols := make([]any, len(o.cols[:positionShift]))
				copy(beforeCols, o.cols[:positionShift])

				afterCols := make([]any, len(o.cols[positionShift:]))
				copy(afterCols, o.cols[positionShift:])

				o.cols = append(beforeCols, relationsCols...)
				o.cols = append(o.cols, afterCols...)

				countShift += len(relationsCols)
				break
			}
		}
	}
}

// GetCols возвращает массив полей
func (o sanitizeOptionHandler) GetCols() []any {
	return o.cols
}

// SanitizeRowsForInsert возвращает объект с полями для добавления сущности
func SanitizeRowsForInsert(entity interface{}, clock clockwork.Clock) (int64, map[string]any) {
	opts := []SanitizeRowsOption{
		WithDefaultTimestamps("created_at", "updated_at"),
	}

	return SanitizeRows(entity, clock, opts...)
}

// SanitizeRowsForUpdate возвращает объект с полями для обновления сущности
func SanitizeRowsForUpdate(entity any, clock clockwork.Clock) (int64, map[string]any) {
	opts := []SanitizeRowsOption{
		WithSkippingFields("created_at"),
		WithDefaultTimestamps("updated_at"),
	}

	return SanitizeRows(entity, clock, opts...)
}

type SanitizeRowsOption func(*sanitizeRowsHandler)

// SanitizeRows возвращает объект с полями для добавления сущности
func SanitizeRows(entity any, clock clockwork.Clock, opts ...SanitizeRowsOption) (int64, map[string]any) {
	handler := &sanitizeRowsHandler{}
	for _, opt := range opts {
		opt(handler)
	}

	vEntity := reflect.ValueOf(entity)

	var primary int64
	rows := map[string]any{}
	for i := 0; i < vEntity.NumField(); i++ {
		tag := vEntity.Type().Field(i).Tag

		if dbFieldName := tag.Get("db"); dbFieldName != "" {
			if pkTag := tag.Get("primary"); pkTag != "" {
				primary = vEntity.Field(i).Int()
				continue
			}

			if _, ok := handler.SkippingFields[dbFieldName]; !ok {
				rows[dbFieldName] = vEntity.Field(i).Interface()
			}
		}
	}

	for _, tsField := range handler.DefaultTimestamps {
		if _, ok := rows[tsField]; ok {
			rows[tsField] = clock.Now()
		}
	}

	return primary, rows
}

func WithSkippingFields(fields ...string) SanitizeRowsOption {
	return func(handler *sanitizeRowsHandler) {
		mapped := make(map[string]bool, len(fields))
		for _, val := range fields {
			mapped[val] = false
		}

		handler.SetSkippingFields(mapped)
	}
}

func WithDefaultTimestamps(fields ...string) SanitizeRowsOption {
	return func(handler *sanitizeRowsHandler) {
		handler.SetDefaultTimestamps(fields)
	}
}

type sanitizeRowsHandler struct {
	SkippingFields    map[string]bool
	DefaultTimestamps []string
}

func (h *sanitizeRowsHandler) SetSkippingFields(val map[string]bool) {
	h.SkippingFields = val
}

func (h *sanitizeRowsHandler) SetDefaultTimestamps(fields []string) {
	h.DefaultTimestamps = fields
}
