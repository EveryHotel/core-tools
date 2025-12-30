package repo

import (
	"reflect"
)

// TODO everyHotel
//  Здесь поддерживаем два вараинта обработки вложенных структур
//  - через inner_struct
//  - через embedded_struct
//  Желательно потом привести это все к одному знаменателю

// SanitizeRowsForInsert возвращает объект с полями для добавления сущности
func SanitizeRowsForInsert[ID int64 | string](entity any) (ID, map[string]any) {
	opts := []SanitizeRowsOption{
		WithDefaultTimestamps("created_at", "updated_at"),
	}

	return SanitizeRows[ID](entity, opts...)
}

// SanitizeRowsForUpdate возвращает объект с полями для обновления сущности
func SanitizeRowsForUpdate[ID int64 | string](entity any) (ID, map[string]any) {
	opts := []SanitizeRowsOption{
		WithSkippingFields("created_at"),
		WithDefaultTimestamps("updated_at"),
	}

	return SanitizeRows[ID](entity, opts...)
}

// SanitizeRowsForUpdateMultiple возвращает объект с полями для обновления сущности
func SanitizeRowsForUpdateMultiple[ID int64 | string](entity interface{}) (ID, map[string]interface{}) {
	opts := []SanitizeRowsOption{
		WithDefaultTimestamps("updated_at"),
	}

	return SanitizeRows[ID](entity, opts...)
}

type SanitizeRowsOption func(*sanitizeRowsHandler)

// SanitizeRows возвращает объект с полями для добавления сущности
func SanitizeRows[ID int64 | string](entity any, opts ...SanitizeRowsOption) (ID, map[string]any) {
	handler := &sanitizeRowsHandler{}
	for _, opt := range opts {
		opt(handler)
	}

	vEntity := reflect.ValueOf(entity)

	var primary ID
	rows := map[string]any{}
	for i := 0; i < vEntity.NumField(); i++ {
		tag := vEntity.Type().Field(i).Tag

		embeddedStruct := tag.Get("embedded_struct")
		if embeddedStruct == "1" {
			embeddedEntity := vEntity.Field(i).Interface()
			_, embeddedRows := SanitizeRows[ID](embeddedEntity, opts...)
			for key, val := range embeddedRows {
				rows[key] = val
			}

			continue
		}

		// если это вложенная структура, мы собираем отдельно все ее поля и добавляем в общий список
		if innerStruct := tag.Get("inner_struct"); innerStruct != "" {
			innerPrimary, innerRows := SanitizeRows[ID](vEntity.Field(i).Interface(), opts...)
			for k, v := range innerRows {
				rows[k] = v
			}

			primary = innerPrimary

			continue
		}

		dbFieldName := tag.Get("db")
		if dbFieldName == "" {
			continue
		}

		if pkTag := tag.Get("primary"); pkTag != "" {
			reflect.TypeOf(primary)
			primary = vEntity.Field(i).Interface().(ID)
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
			rows[tsField] = "now()"
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
func IsSoftDeletingEntity(entity any) bool {
	vEntity := reflect.ValueOf(entity)

	fieldValue, found := vEntity.Type().FieldByName("DeletedAt")
	if !found {
		return false
	}

	return fieldValue.Tag.Get("db") != ""
}
