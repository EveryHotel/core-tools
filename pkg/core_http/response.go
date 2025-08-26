package coreHttp

import (
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
)

// RenderResponse преобразует и пишет ответ в виде json и выставляет соответствующий статус для response
func RenderResponse(w http.ResponseWriter, response interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logrus.WithFields(logrus.Fields{
			"response": response,
		}).Error("json response encode error", err)
	}
}
