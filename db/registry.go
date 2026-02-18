package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/easynow112/dbkit/config"
)

type DBFactory func(ctx context.Context, cfg *config.Config, target string) (DB, error)

type DriverFactory func(ctx context.Context, driverCfg *config.DriverConfig, globalCfg *config.GlobalConfig) (DB, error)

var (
	drivers = map[string]DriverFactory{}
	mu      sync.RWMutex
)

func RegisterDriver(name string, factory DriverFactory) {
	mu.Lock()
	defer mu.Unlock()

	if _, ok := drivers[name]; ok {
		panic(fmt.Sprintf("db: driver already registered: %s", name))
	}
	drivers[name] = factory
}

func NewDB(ctx context.Context, config *config.Config, target string) (DB, error) {
	driverConfig, ok := config.Databases[target]
	if !ok {
		return nil, fmt.Errorf("db definition missing: '%s'", target)
	}
	factory, ok := drivers[driverConfig.Driver]
	if !ok {
		return nil, fmt.Errorf("unknown db driver: %s", driverConfig.Driver)
	}
	return factory(ctx, &driverConfig, &config.Global)
}
