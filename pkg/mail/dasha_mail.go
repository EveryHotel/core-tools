package mail

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
)

const (
	dashaApiUrl = "https://api.dashamail.ru/"
)

type dashaMailService struct {
	apiKey string
}

func NewDashaMailService(apiKey string) MailService {
	return &dashaMailService{
		apiKey: apiKey,
	}
}

// Send Отправляет письмо
func (s *dashaMailService) Send(email EmailMessage) error {
	var toEmails []string
	for _, to := range email.To {
		toEmails = append(toEmails, to.String())
	}

	attaches := make(map[string]Attachment)
	for i := range email.Attachments {
		attaches[fmt.Sprintf("attachments[%d]", i)] = email.Attachments[i]
	}
	params := map[string]string{
		"method":     "transactional.send",
		"api_key":    s.apiKey,
		"from_email": email.From.Mail,
		"to":         strings.Join(toEmails, ","),
		"subject":    email.Subject,
		"message":    email.Body,
		"format":     "json",
	}

	form, contentType, err := prepareForm(params, attaches)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"params": params,
		}).Error(fmt.Sprintf("cant'n form email: ", err))
		return err
	}

	req, err := http.NewRequest(http.MethodPost, dashaApiUrl, &form)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", contentType)

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

func prepareForm(fields map[string]string, values map[string]Attachment) (bytes.Buffer, string, error) {
	var b bytes.Buffer
	var err error
	writer := multipart.NewWriter(&b)
	for k, v := range fields {
		var fw io.Writer
		if fw, err = writer.CreateFormField(k); err != nil {
			return b, "", err
		}
		if _, err = fw.Write([]byte(v)); err != nil {
			return b, "", err
		}
	}
	for k, v := range values {
		var fw io.Writer

		if fw, err = writer.CreateFormFile(k, v.Filename); err != nil {
			return b, "", err
		}

		if _, err = fw.Write(v.Content); err != nil {
			return b, "", err
		}
	}
	if err = writer.Close(); err != nil {
		return b, "", err
	}

	return b, writer.FormDataContentType(), nil
}
