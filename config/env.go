package config

import (
	"fmt"
	"os"
	"regexp"
)

func expandEnvConfig(config *Config) error {

	if err := expandEnvStrings(
		&config.Active.Source.Migrations.Up,
		&config.Active.Source.Migrations.Down,
		&config.Active.Source.Seeds,
		&config.Active.Database,
	); err != nil {
		return err
	}

	if err := expandEnvDriverConfigMap(config.Sources); err != nil {
		return err
	}

	if err := expandEnvDriverConfigMap(config.Databases); err != nil {
		return err
	}

	return nil
}

func expandEnvDriverConfigMap(input map[string]DriverConfig) error {
	for key, value := range input {
		if err := expandEnv(&value.Driver); err != nil {
			return err
		}
		if err := expandEnvMap(value.Config); err != nil {
			return err
		}
		input[key] = value
	}
	return nil
}

func expandEnvMap(input map[string]string) error {
	for key, value := range input {
		if err := expandEnv(&value); err != nil {
			return err
		}
		input[key] = value
	}
	return nil
}

func expandEnvStrings(strings ...*string) error {
	for _, s := range strings {
		if err := expandEnv(s); err != nil {
			return err
		}
	}
	return nil
}

func expandEnv(input *string) error {
	if input == nil {
		return fmt.Errorf("env string pointer is nil")
	}

	var err error

	envRegExp := regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`) // Regex to match ${VAR_NAME}

	var missing []string
	expanded := envRegExp.ReplaceAllStringFunc(*input, func(match string) string {
		submatches := envRegExp.FindStringSubmatch(match)
		if len(submatches) < 2 {
			err = fmt.Errorf("malformed regexp submatches: %s", submatches)
			return ""
		}
		envVar := submatches[1]
		value, ok := os.LookupEnv(envVar)
		if !ok {
			missing = append(missing, envVar)
			return ""
		}
		return value
	})

	if len(missing) > 0 {
		return fmt.Errorf("missing environment variable(s): %v", missing)
	} else if err != nil {
		return err
	}

	*input = expanded
	return nil
}
