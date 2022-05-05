package database

import (
	"fmt"
	"reflect"
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
