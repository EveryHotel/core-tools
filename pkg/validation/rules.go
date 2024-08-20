package validation

import (
	"reflect"

	"github.com/go-ozzo/ozzo-validation/v4"

	"github.com/EveryHotel/core-tools/pkg/types"
)

// Nested хэлпер для применения валидации вложенных структур
func Nested(target interface{}, fieldRules ...*validation.FieldRules) *validation.FieldRules {
	return validation.Field(target, validation.By(func(value interface{}) error {
		if nestedField, ok := target.(validation.Validatable); ok {
			return nestedField.Validate()
		}

		valueV := reflect.Indirect(reflect.ValueOf(value))
		if valueV.CanAddr() {
			addr := valueV.Addr().Interface()
			return validation.ValidateStruct(addr, fieldRules...)
		}

		return validation.ValidateStruct(target, fieldRules...)
	}))
}

// OmitOrNotEmpty поле либо не представлено в запросе, либо должно быть заполнено
func OmitOrNotEmpty[T any](value interface{}) error {
	val, ok := value.(types.Omitempty[T])
	if !ok || !val.Valid {
		return nil
	}

	if validation.IsEmpty(val.Value) {
		return validation.ErrRequired
	}

	return nil
}
