package fs

import (
	"fmt"

	"github.com/easynow112/dbkit/config"
)

type Config struct {
	Dir string
}

func newConfig(portConfig *config.DriverConfig) (config *Config, err error) {
	rawConfig := portConfig.Config
	dir, err := configString(rawConfig, "dir")
	if err != nil {
		return nil, err
	}
	return &Config{
		Dir: dir,
	}, nil
}

func configString(rawConfig map[string]string, key string) (string, error) {
	value, ok := rawConfig[key]
	if !ok {
		return "", fmt.Errorf("fs driver requires '%s' string in config", key)
	}
	return value, nil
}
