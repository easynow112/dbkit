package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
)

type DB struct {
	mu          sync.Mutex
	connections int
	sqlDB       *sql.DB
}

func (db *DB) AcquireConnection(ctx context.Context) (db.Connection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	conn, err := db.sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	db.connections++
	return &Connection{
		conn: conn,
		db:   db,
	}, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.connections > 0 {
		return fmt.Errorf("connection pool is not empty")
	}
	return db.sqlDB.Close()
}

func NewDB(ctx context.Context, driverCfg *config.DriverConfig, globalConfig *config.GlobalConfig) (db.DB, error) {
	if driverCfg == nil {
		return nil, fmt.Errorf("driver config is nil")
	}
	if driverCfg.Driver != "sqlite" {
		return nil, fmt.Errorf("invalid driver: %s", driverCfg.Driver)
	}

	cfg, err := newConfig(driverCfg, globalConfig.BaseDir)
	if err != nil {
		return nil, err
	}

	sqlDB, err := sql.Open("sqlite", cfg.DSN)
	if err != nil {
		return nil, err
	}

	return &DB{
		sqlDB: sqlDB,
	}, nil
}
