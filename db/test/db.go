package test

import (
	"context"
	"fmt"
	"sync"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
)

type DB struct {
	mu          sync.Mutex
	connections int
	closed      bool
	store       db.AppliedMigrationStore
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.connections != 0 {
		return fmt.Errorf("connection pool is not empty")
	}
	db.closed = true
	return nil
}

func (db *DB) AcquireConnection(ctx context.Context) (db.Connection, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}
	db.connections++
	return &Connection{
		db: db,
	}, nil
}

func NewFactory(store db.AppliedMigrationStore) db.DriverFactory {
	return func(ctx context.Context, _ *config.DriverConfig, _ *config.GlobalConfig) (db.DB, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return &DB{
			store:       store,
			connections: 0,
			closed:      false,
		}, nil
	}
}
