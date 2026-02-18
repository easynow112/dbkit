package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

const FilePath = "dbkit.json"

func LoadConfig() (config *Config, err error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	absCwdPath, err := filepath.Abs(cwd)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filepath.Join(absCwdPath, FilePath))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config = &Config{
		Global: GlobalConfig{
			BaseDir: absCwdPath,
		},
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		return nil, err
	}

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}

	if envPath, ok := config.Environments[config.Active.Environment]; ok {
		err := godotenv.Load(envPath)
		if err != nil {
			return nil, err
		}
	}

	err = expandEnvConfig(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
