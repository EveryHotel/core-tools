package validation

import (
	"errors"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func DecimalFunc(check bool, message string) validation.RuleFunc {
	return func(value interface{}) error {
		if !check {
			if message == "" {
				message = "incorrect decimal value"
			}
			return errors.New(message)
		}
		return nil
	}
}
