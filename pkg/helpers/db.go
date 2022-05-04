package helpers

import (
	"net/url"
	"strings"
)

type DBConfig struct {
	Scheme   string
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// GetDBConfig парсит строчку соединения типа [scheme:][//[userinfo@]host][/]path[?query][#fragment] и возвращает DBConfig
func GetDBConfig(connection string) (DBConfig, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return DBConfig{}, err
	}

	hostParts := strings.Split(u.Host, ":")

	config := DBConfig{
		Scheme:   u.Scheme,
		Host:     hostParts[0],
		User:     u.User.Username(),
		Database: strings.Trim(u.Path, "/"),
	}

	if len(hostParts) == 2 {
		config.Port = hostParts[1]
	}

	pass, exists := u.User.Password()
	if exists {
		config.Password = pass
	}

	return config, nil
}
