package config

import (
	"fmt"
)

type Migrations struct {
	Up   string `json:"up"`
	Down string `json:"down"`
}

type Source struct {
	Migrations Migrations `json:"migrations"`
	Seeds      string     `json:"seeds"`
}

type ActiveConfig struct {
	Source      Source `json:"source"`
	Database    string `json:"database"`
	Environment string `json:"environment"`
}

type Config struct {
	Active       ActiveConfig
	Environments map[string]string
	Databases    map[string]DriverConfig
	Sources      map[string]DriverConfig
	Global       GlobalConfig
}

type ConfigFactory func() (*Config, error)

func (c *Config) validate() error {
	// Environments
	if c.Active.Environment != "" {
		if _, ok := c.Environments[c.Active.Environment]; !ok {
			return fmt.Errorf("%s is not a valid environment driver", c.Active.Environment)
		}
	}

	// Databases
	if err := validateDriverConfig("active.database", c.Databases, c.Active.Database); err != nil {
		return err
	}

	// Sources
	if err := validateDriverConfig("active.source.migrations.up", c.Sources, c.Active.Source.Migrations.Up); err != nil {
		return err
	}
	if err := validateDriverConfig("active.source.migrations.down", c.Sources, c.Active.Source.Migrations.Down); err != nil {
		return err
	}
	if err := validateDriverConfig("active.source.seeds", c.Sources, c.Active.Source.Seeds); err != nil {
		return err
	}

	// Global
	if err := c.Global.validate(); err != nil {
		return err
	}

	return nil
}

func validateDriverConfig(portName string, configMap map[string]DriverConfig, driver string) error {
	if driver == "" {
		return fmt.Errorf("%s driver is missing", portName)
	}
	portConfig, ok := configMap[driver]
	if !ok {
		return fmt.Errorf("%s is not a valid %s config", driver, portName)
	}
	if err := portConfig.validate(); err != nil {
		return fmt.Errorf("%s is not a valid %s config: %v", driver, portName, err)
	}
	return nil
}

type DriverConfig struct {
	Driver string            `json:"driver"`
	Config map[string]string `json:"config"`
}

func (p *DriverConfig) validate() error {
	if p.Driver == "" {
		return fmt.Errorf("driver is required")
	}
	if p.Config == nil {
		return fmt.Errorf("config is required")
	}
	return nil
}

type GlobalConfig struct {
	BaseDir string
}

func (g *GlobalConfig) validate() error {
	if g.BaseDir == "" {
		return fmt.Errorf("baseDir is required")
	}
	return nil
}
