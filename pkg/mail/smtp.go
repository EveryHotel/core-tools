package mail

import (
	mail "github.com/xhit/go-simple-mail/v2"
)

type smtpMailService struct {
	user     string
	password string
	host     string
	port     int
	from     string
}

func NewSmtpMailService(user string, pass string, host string, port int) MailService {
	return &smtpMailService{
		user:     user,
		password: pass,
		host:     host,
		port:     port,
	}
}

// Send отправляет письмо через смтп сервер
func (s *smtpMailService) Send(email EmailMessage) error {
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

	server := mail.NewSMTPClient()

	server.Host = s.host
	server.Port = s.port
	server.Username = s.user
	server.Password = s.password

	server.Authentication = mail.AuthPlain

	client, err := server.Connect()

	if err != nil {
		server.Authentication = mail.AuthNone
		client, err = server.Connect()
		if err != nil {
			return err
		}
	}

	return msg.Send(client)
}
