package pg

import (
	"fmt"
	"strconv"

	"github.com/easynow112/dbkit/config"
)

type Config struct {
	Host     string
	Port     int
	Name     string
	Password string
	User     string
	SSL      string
}

func newConfig(portConfig *config.DriverConfig) (config *Config, err error) {

	rawConfig := portConfig.Config

	host, err := configString(rawConfig, "host")
	if err != nil {
		return nil, err
	}

	port, err := configInt(rawConfig, "port")
	if err != nil {
		return nil, err
	}

	name, err := configString(rawConfig, "name")
	if err != nil {
		return nil, err
	}

	password, err := configString(rawConfig, "password")
	if err != nil {
		return nil, err
	}

	user, err := configString(rawConfig, "user")
	if err != nil {
		return nil, err
	}

	ssl, err := configString(rawConfig, "ssl")
	if err != nil {
		return nil, err
	}

	return &Config{
		Host:     host,
		Port:     port,
		Name:     name,
		Password: password,
		User:     user,
		SSL:      ssl,
	}, nil
}

func configString(rawConfig map[string]string, key string) (string, error) {
	value, ok := rawConfig[key]
	if !ok {
		return "", fmt.Errorf("pg driver requires '%s' string in config", key)
	}
	return value, nil
}

func configInt(rawConfig map[string]string, key string) (int, error) {
	strVal, ok := rawConfig[key]
	if !ok {
		return 0, fmt.Errorf("pg driver requires '%s' int in config", key)
	}
	parsed, err := strconv.Atoi(strVal)
	if err != nil {
		return 0, fmt.Errorf("pg driver expects '%s' to be an int, received: %s", key, strVal)
	}
	return parsed, nil
}
