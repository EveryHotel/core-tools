package mail

import (
	"context"
	"log/slog"

	mail "github.com/xhit/go-simple-mail/v2"
)

type smtpMailService struct {
	clientConfig *mail.SMTPServer
}

func NewSmtpMailService(clientConfig *mail.SMTPServer) MailService {
	return &smtpMailService{
		clientConfig: clientConfig,
	}
}

// Send отправляет письмо через smtp сервер
func (s *smtpMailService) Send(ctx context.Context, email EmailMessage) error {
	msg := mail.NewMSG()
	msg.SetFrom(email.From.String()).
		SetSubject(email.Subject)

	mailType := mail.TextPlain
	if email.IsBodyHtml {
		mailType = mail.TextHTML
	}
	msg.SetBody(mailType, email.Body)

	for _, address := range email.To {
		msg.AddTo(address.String())
	}
	for _, address := range email.Cc {
		msg.AddCc(address.String())
	}
	for _, address := range email.Bcc {
		msg.AddBcc(address.String())
	}

	for _, attach := range email.Attachments {
		msg.AddAttachmentData(attach.Content, attach.Filename, attach.MimeType)
	}

	switch email.Priority {
	case PriorityLow:
		msg.SetPriority(mail.PriorityLow)
		break
	case PriorityHigh:
		msg.SetPriority(mail.PriorityHigh)
		break
	}

	client, err := s.clientConfig.Connect()
	if err != nil {
		slog.ErrorContext(ctx, "smtp connect error",
			slog.Any("error", err))
		return err
	}

	if err = msg.Send(client); err != nil {
		slog.ErrorContext(ctx, "smtp send error",
			slog.Any("error", err))
		return err
	}

	return nil
}
