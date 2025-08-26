package mail

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/smtp"
	"strings"
)

// TODO  everyHotel
//  В метод прокинут ctx для соответствия интерфейсу
//  Надо будет при обновлении поправить код

type smtpNoAuthMailService struct {
	hostPort string
}

func NewSmtpNoAuthMailService(hostPort string) MailService {
	return &smtpNoAuthMailService{
		hostPort: hostPort,
	}
}

// Send отправляет письмо через смтп сервер
func (s *smtpNoAuthMailService) Send(ctx context.Context, email EmailMessage) error {

	var toMails []string
	for _, to := range email.To {
		toMails = append(toMails, to.String())
	}

	if err := sendMail(s.hostPort, email.From.Mail, email.Subject, email.Body, toMails); err != nil {
		return fmt.Errorf("err to mails: %v", err)
	}

	var ccMails []string
	for _, cc := range email.Cc {
		ccMails = append(ccMails, cc.String())
	}

	if len(ccMails) > 0 {
		if err := sendMail(s.hostPort, email.From.Mail, email.Subject, email.Body, ccMails); err != nil {
			return fmt.Errorf("err cc mails: %v", err)
		}
	}

	return nil
}

func sendMail(addr, from, subject, body string, to []string) error {
	r := strings.NewReplacer("\r\n", "", "\r", "", "\n", "", "%0a", "", "%0d", "")

	c, err := smtp.Dial(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if err = c.Mail(r.Replace(from)); err != nil {
		return err
	}
	for i := range to {
		to[i] = r.Replace(to[i])
		if err = c.Rcpt(to[i]); err != nil {
			return err
		}
	}

	w, err := c.Data()
	if err != nil {
		return err
	}

	msg := "To: " + strings.Join(to, ",") + "\r\n" +
		"From: " + from + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"Content-Type: text/html; charset=\"UTF-8\"\r\n" +
		"Content-Transfer-Encoding: base64\r\n" +
		"\r\n" + base64.StdEncoding.EncodeToString([]byte(body))

	_, err = w.Write([]byte(msg))
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return c.Quit()
}
