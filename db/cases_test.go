package db_test

import (
	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
	"github.com/easynow112/dbkit/db/pg"
	"github.com/easynow112/dbkit/db/test"
)

type driverCase struct {
	factory db.DriverFactory
	config  config.DriverConfig
}

func getDriverCases() []driverCase {
	return []driverCase{
		{
			factory: pg.NewDB,
			config: config.DriverConfig{
				Driver: "pg",
				Config: map[string]string{
					"host":     "127.0.0.1",
					"port":     "5432",
					"user":     "user",
					"password": "password",
					"name":     "database",
					"ssl":      "prefer",
				},
			},
		},
		{
			factory: test.NewFactory(test.NewStore(map[string]*db.AppliedMigration{})),
			config: config.DriverConfig{
				Driver: "test",
			},
		},
	}
}
