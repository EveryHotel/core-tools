package validation

import (
	"reflect"

	"github.com/go-ozzo/ozzo-validation/v4"
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
