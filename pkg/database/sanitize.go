package database

import (
	"fmt"
	"reflect"
	"time"
)

type SanitizeOption func(*sanitizeOptionHandler)

type sanitizeOptionHandler struct {
	cols []interface{}
}

// Sanitize возвращает список доступных DB полей у предоставленной структуры
func Sanitize(dest interface{}, opts ...SanitizeOption) []interface{} {
	vDest := reflect.ValueOf(dest)
	var cols []interface{}

	for i := 0; i < vDest.NumField(); i++ {
		typeField := vDest.Type().Field(i)
		tag := typeField.Tag

		if tagVal := tag.Get("db"); tagVal != "" {
			cols = append(cols, tagVal)
		}
	}

	optHandler := newSanitizeOptionHandler(cols)
	for _, opt := range opts {
		opt(optHandler)
	}

	return optHandler.GetCols()
}

func newSanitizeOptionHandler(cols []interface{}) *sanitizeOptionHandler {
	return &sanitizeOptionHandler{
		cols: cols,
	}
}

// WithPrefix возвращает опцию для задания префикса полей
func WithPrefix(p string) SanitizeOption {
	return func(o *sanitizeOptionHandler) {
		o.ApplyPrefix(p)
	}
}

// ApplyPrefix применяет префикс ко всем полям
func (o *sanitizeOptionHandler) ApplyPrefix(p string) {
	for i, col := range o.cols {
		o.cols[i] = fmt.Sprintf("%s.%s", p, col)
	}
}

// GetCols возвращает массив полей
func (o sanitizeOptionHandler) GetCols() []interface{} {
	return o.cols
}

// SanitizeRowsForInsert возвращает объект с полями для добавления сущности
func SanitizeRowsForInsert(entity interface{}) (int64, map[string]interface{}) {
	opts := []SanitizeRowsOption{
		WithDefaultTimestamps("created_at", "updated_at"),
	}

	return SanitizeRows(entity, opts...)
}

// SanitizeRowsForUpdate возвращает объект с полями для обновления сущности
func SanitizeRowsForUpdate(entity interface{}) (int64, map[string]interface{}) {
	opts := []SanitizeRowsOption{
		WithSkippingFields("created_at"),
		WithDefaultTimestamps("updated_at"),
	}

	return SanitizeRows(entity, opts...)
}

type SanitizeRowsOption func(*sanitizeRowsHandler)

// SanitizeRows возвращает объект с полями для добавления сущности
func SanitizeRows(entity interface{}, opts ...SanitizeRowsOption) (int64, map[string]interface{}) {
	handler := &sanitizeRowsHandler{}
	for _, opt := range opts {
		opt(handler)
	}

	vEntity := reflect.ValueOf(entity)

	var primary int64
	rows := map[string]interface{}{}
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
			rows[tsField] = time.Now()
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
