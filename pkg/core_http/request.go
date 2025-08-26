package coreHttp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/sirupsen/logrus"
)

// ValidateRequestBody парсит тело запроса и записывает его в предоставленную структуру, если запрос
// валидируется, то запускает его валидацию
func ValidateRequestBody(r *http.Request, req interface{}) error {
	if err := ParseRequestBody(r.Body, req); err != nil {
		logrus.WithFields(logrus.Fields{
			"body": r.Body,
		}).Error("cannot parse body", err)

		return err
	}

	if validatedReq, ok := req.(validation.Validatable); ok {
		if err := validatedReq.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ParseRequestBody парсит тело запроса и записывает его в предоставленную структуру
func ParseRequestBody(body io.Reader, req interface{}) error {
	decoder := json.NewDecoder(body)
	err := decoder.Decode(req)

	// empty body
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}
