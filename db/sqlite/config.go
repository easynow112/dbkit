package sqlite

import (
	"fmt"
	"path/filepath"

	"github.com/easynow112/dbkit/config"
)

type Config struct {
	DSN string
}

func newConfig(portConfig *config.DriverConfig, baseDir string) (*Config, error) {
	rawConfig := portConfig.Config
	path, err := configString(rawConfig, "path")
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, filepath.ToSlash(path))
	}
	return &Config{
		DSN: fmt.Sprintf("file:%s", path),
	}, nil
}

func configString(rawConfig map[string]string, key string) (string, error) {
	value, ok := rawConfig[key]
	if !ok {
		return "", fmt.Errorf("sqlite driver requires '%s' string in config", key)
	}
	return value, nil
}
