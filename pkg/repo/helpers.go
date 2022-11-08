package repo

import (
	"reflect"
	"time"
)

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

// SanitizeRowsForUpdateMultiple возвращает объект с полями для обновления сущности
func SanitizeRowsForUpdateMultiple(entity interface{}) (int64, map[string]interface{}) {
	opts := []SanitizeRowsOption{
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

		dbFieldName := tag.Get("db")
		if dbFieldName == "" {
			continue
		}

		if pkTag := tag.Get("primary"); pkTag != "" {
			primary = vEntity.Field(i).Int()
			// если поле помечено как НЕ автоинкрементное, оставляем его в списке
			if nsTag := tag.Get("not_serial"); nsTag == "" {
				continue
			}
		}

		if _, ok := handler.SkippingFields[dbFieldName]; !ok {
			rows[dbFieldName] = vEntity.Field(i).Interface()
		}
	}

	for _, tsField := range handler.DefaultTimestamps {
		if _, ok := rows[tsField]; ok {
			rows[tsField] = time.Now()
		}
	}

	return primary, rows
}

// WithSkippingFields пропустить поля
func WithSkippingFields(fields ...string) SanitizeRowsOption {
	return func(handler *sanitizeRowsHandler) {
		mapped := make(map[string]bool, len(fields))
		for _, val := range fields {
			mapped[val] = false
		}

		handler.SetSkippingFields(mapped)
	}
}

// WithDefaultTimestamps проставить выбранные timestamps в NOW
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

// IsSoftDeletingEntity проверяет является ли сущность доступной для soft удаления
func IsSoftDeletingEntity(entity interface{}) bool {
	vEntity := reflect.ValueOf(entity)

	fieldValue, found := vEntity.Type().FieldByName("DeletedAt")
	if !found {
		return false
	}

	return fieldValue.Tag.Get("db") != ""
}
