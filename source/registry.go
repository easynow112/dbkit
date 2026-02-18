package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/easynow112/dbkit/config"
)

type StoreFactory func(ctx context.Context, config *config.Config, target string) (Store, error)

type DriverFactory func(ctx context.Context, driverCfg *config.DriverConfig, globalCfg *config.GlobalConfig) (Store, error)

var (
	drivers = map[string]DriverFactory{}
	mu      sync.RWMutex
)

func RegisterDriver(name string, factory DriverFactory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := drivers[name]; ok {
		panic(fmt.Sprintf("source driver already registered: %s", name))
	}
	drivers[name] = factory
}

func NewStore(ctx context.Context, config *config.Config, target string) (Store, error) {
	driverConfig, ok := config.Sources[target]
	if !ok {
		return nil, fmt.Errorf("source driver definition missing: '%s'", target)
	}
	factory, ok := drivers[driverConfig.Driver]
	if !ok {
		return nil, fmt.Errorf("unknown source driver: %s", driverConfig.Driver)
	}
	return factory(ctx, &driverConfig, &config.Global)
}
