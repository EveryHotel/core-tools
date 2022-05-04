package sberidentity

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

const (
	authPath              = "connect/token"
	ScopeNotificationMail = "notification:mail"
	ScopeNotificationSms  = "notification:sms"
)

type SberIdentityResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
	Scope       string `json:"scope"`
}

type SberIdentityService interface {
	Identity(scopes []string) (SberIdentityResponse, error)
}

type sberIdentityService struct {
	ApiUrl   string
	ClientId string
	Secret   string
}

func NewSberIdentityService(apiUrl string, clientId string, secret string) SberIdentityService {
	return &sberIdentityService{
		ApiUrl:   apiUrl,
		ClientId: clientId,
		Secret:   secret,
	}
}

//Identity получиет токен для доступа к сервисам сбера с переданными scopes
func (s *sberIdentityService) Identity(scopes []string) (SberIdentityResponse, error) {
	data := url.Values{}
	data.Set("client_id", s.ClientId)
	data.Set("client_secret", s.Secret)
	data.Set("scope", strings.Join(scopes, ","))
	data.Set("response_type", "token")
	data.Set("grant_type", "client_credentials")

	u, _ := url.ParseRequestURI(s.ApiUrl)
	u.Path = authPath
	urlStr := u.String()

	client := &http.Client{}
	r, _ := http.NewRequest(http.MethodPost, urlStr, strings.NewReader(data.Encode()))
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	response, err := client.Do(r)
	if err != nil {
		return SberIdentityResponse{}, err
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		ar := SberIdentityResponse{}

		if err := json.Unmarshal(body, &ar); err != nil {
			return ar, err
		}

		return ar, nil
	}

	return SberIdentityResponse{}, errors.New(fmt.Sprintf("mail auth error statusCode: %d", response.StatusCode))
}
