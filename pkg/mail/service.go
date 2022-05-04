package mail

import (
	"fmt"
)

const (
	CategoryNone        = "None"
	CategorySecurity    = "Security"
	CategorySystem      = "System"
	CategoryApplication = "Application"
	CategoryInformation = "Information"

	PriorityNormal = "Normal"
	PriorityLow    = "Low"
	PriorityHigh   = "High"
)

type Address struct {
	Mail        string `json:"mail"`
	DisplayName string `json:"displayName"`
}

type Attachment struct {
	Filename  string `json:"filename"`
	Content   []byte `json:"content"`
	MimeType  string `json:"mimeType"`
	ContentId string `json:"contentId"`
}

type EmailMessage struct {
	SenderId    int64        `json:"senderId"`
	Category    string       `json:"category"`
	Priority    string       `json:"priority"`
	IsBodyHtml  bool         `json:"isBodyHtml"`
	From        Address      `json:"from"`
	To          []Address    `json:"to"`
	Cc          []Address    `json:"cc"`
	Bcc         []Address    `json:"bcc"`
	ReplyTo     []Address    `json:"replyTo"`
	Subject     string       `json:"subject"`
	Body        string       `json:"body"`
	Attachments []Attachment `json:"attachments"`
}

type MailService interface {
	Send(email EmailMessage) error
}

//String преобразует адрес к формату rfc 822
func (a *Address) String() string {
	if a.DisplayName != "" {
		return fmt.Sprintf("%s <%s>", a.DisplayName, a.Mail)
	}

	return a.Mail
}

type DefaultConfig struct {
	AppName  string
	BaseUrl  string
	FromMail string
}
