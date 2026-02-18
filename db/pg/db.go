package pg

import (
	"context"
	"fmt"
	"sync"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	mu          sync.Mutex
	connections int
	pgxPool     *pgxpool.Pool
}

func (db *DB) AcquireConnection(ctx context.Context) (db.Connection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	conn, err := db.pgxPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	db.connections++
	return &Connection{
		pgxConn: conn,
		db:      db,
	}, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.connections > 0 {
		return fmt.Errorf("connection pool is not empty")
	}
	db.pgxPool.Close()
	return nil
}

func NewDB(ctx context.Context, driverCfg *config.DriverConfig, _ *config.GlobalConfig) (db.DB, error) {
	if driverCfg == nil {
		return nil, fmt.Errorf("driver config is nil")
	}
	if driverCfg.Driver != "pg" {
		return nil, fmt.Errorf("invalid driver: %s", driverCfg.Driver)
	}

	pgConfig, err := newConfig(driverCfg)
	if err != nil {
		return nil, err
	}

	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d dbname=%s sslmode=%s",
		pgConfig.User,
		pgConfig.Password,
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.Name,
		pgConfig.SSL,
	)
	pgxConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return &DB{
		pgxPool: pool,
	}, nil
}
