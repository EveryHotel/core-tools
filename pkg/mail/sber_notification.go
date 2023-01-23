package mail

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"

	sberIdentity "git.esphere.local/SberbankTravel/hotels/core-tools/pkg/sber-identity"
)

const (
	mailPath = "message/mail"
)

type sberMailService struct {
	identityService sberIdentity.SberIdentityService
	apiUrl          string
	defaultSenderId int64
	token           string
	tokenExpiredAt  int64
}

func NewSberMailService(identityService sberIdentity.SberIdentityService, apiUrl string, defaultSenderId int64) MailService {
	return &sberMailService{
		identityService: identityService,
		apiUrl:          apiUrl,
		defaultSenderId: defaultSenderId,
	}
}

// Send Отправляет письмо через апи сбера
func (s *sberMailService) Send(email EmailMessage) error {
	if s.token == "" || s.tokenExpiredAt < time.Now().Unix() {
		response, err := s.identityService.Identity([]string{sberIdentity.ScopeNotificationMail})
		if err != nil {
			return err
		}
		s.tokenExpiredAt = time.Now().Unix() + response.ExpiresIn
		s.token = response.AccessToken
	}

	if email.SenderId == 0 {
		email.SenderId = s.defaultSenderId
	}
	if email.Category == "" {
		email.Category = CategoryNone
	}
	if email.Priority == "" {
		email.Priority = PriorityNormal
	}

	for i, attach := range email.Attachments {
		if attach.ContentId == "" {
			attach.ContentId = fmt.Sprintf("attach_%d", i)
		}
	}

	data, err := json.Marshal(email)
	if err != nil {
		return err
	}

	u, _ := url.ParseRequestURI(s.apiUrl)
	u.Path = mailPath
	urlStr := u.String()
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+s.token)

	client := &http.Client{}

	response, err := client.Do(req)
	if err != nil {
		return err
	}

	if response.StatusCode < 200 || response.StatusCode > 300 {
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		logrus.WithFields(logrus.Fields{
			"status": response.StatusCode,
		}).Error(body)
		return errors.New(fmt.Sprintf("mail sending error statusCode: %d, %s", response.StatusCode, body))
	}

	return nil
}
