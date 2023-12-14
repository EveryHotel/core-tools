package mail

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"strings"
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

type dashaResponse struct {
	Response struct {
		Msg struct {
			ErrCode int    `json:"err_code"`
			Text    string `json:"text"`
			Type    string `json:"type"`
		} `json:"msg"`
		Data struct {
			TransactionId string `json:"transaction_id"`
		} `json:"data"`
	} `json:"response"`
}

// Send Отправляет письмо
func (s *dashaMailService) Send(ctx context.Context, email EmailMessage) error {
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
		slog.ErrorContext(ctx, "can't form email",
			slog.Any("error", err),
			slog.Group("params",
				slog.String("method", "transactional.send"),
				slog.String("api_key", s.apiKey),
				slog.String("from_email", email.From.Mail),
				slog.String("to", strings.Join(toEmails, ",")),
				slog.String("subject", email.Subject),
				slog.String("message", email.Body),
				slog.String("format", "json"),
			),
		)

		return err
	}
	response, err := s.doRequest(ctx, form, contentType)
	if err != nil {
		slog.ErrorContext(ctx, "dashamail dorequest",
			slog.Any("error", err))
		return err
	}
	if response.Response.Msg.ErrCode > 0 {
		err = fmt.Errorf("dashamail transaction: %s, error: %s, ", response.Response.Msg.Text, response.Response.Data.TransactionId)
		slog.ErrorContext(ctx, "dashamail transaction send",
			slog.Group("response",
				slog.String("transaction_id", response.Response.Data.TransactionId),
				slog.String("text", response.Response.Msg.Text),
				slog.Int("err_code", response.Response.Msg.ErrCode),
				slog.String("type", response.Response.Msg.Type),
			))
		return err
	}
	return nil
}

func (s *dashaMailService) doRequest(ctx context.Context, form bytes.Buffer, formContentType string) (dashaResponse, error) {
	req, err := http.NewRequest(http.MethodPost, dashaApiUrl, &form)
	if err != nil {
		return dashaResponse{}, err
	}

	req.Header.Add("Content-Type", formContentType)

	client := &http.Client{}

	response, err := client.Do(req)
	if err != nil {
		return dashaResponse{}, err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode > 300 {
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return dashaResponse{}, errors.New(fmt.Sprintf("mail sending error statusCode: %d, %s", response.StatusCode, body))
	}

	var apiResponse dashaResponse
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return dashaResponse{}, fmt.Errorf("dashamail: can't fetch response %v", err)
	}
	if err = json.Unmarshal(responseBody, &apiResponse); err != nil {
		return dashaResponse{}, fmt.Errorf("dashamail: can't unmarshal response %v", err)
	}
	return apiResponse, nil
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
