package coreHttp

import (
	"net/http"
)

// Endpoint функция, которая является http точкой входа
type Endpoint func(w http.ResponseWriter, r *http.Request) error

// ErrApiHandler функция, которая либо обрабатывает переданный тип ошибки, либо возвращает ее обратно
type ErrApiHandler func(http.ResponseWriter, error) error

type ApiEndpointCreator interface {
	Create(Endpoint) http.HandlerFunc
}

// NewApiEndpoint принимает все возможные обработчики ошибок и возвращает структуру, которая создает базовые
// точки входа
func NewApiEndpoint(errorHandlers ...ErrApiHandler) ApiEndpointCreator {
	return &apiEndpointCreator{
		errorHandlers: errorHandlers,
	}
}

type apiEndpointCreator struct {
	errorHandlers []ErrApiHandler
}

// Create создает новую точку входа
func (e apiEndpointCreator) Create(endpoint Endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := endpoint(w, r)
		if err == nil {
			return
		}

		for _, errorHandler := range e.errorHandlers {
			if err = errorHandler(w, err); err == nil {
				return
			}
		}
	}
}
