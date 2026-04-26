package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/easynow112/dbkit/db"
)

type Connection struct {
	db            *DB
	conn          *sql.Conn
	closed        atomic.Bool
	trxInProgress atomic.Bool
}

func (c *Connection) TryAcquireLock(ctx context.Context) (db.Lock, error) {
	if c.closed.Load() {
		return nil, fmt.Errorf("connection is closed")
	}

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	rollback := func(e error) (db.Lock, error) {
		_ = tx.Rollback()
		return nil, e
	}

	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 0),
		locked BOOLEAN NOT NULL,
		owner INTEGER,
		lock_expires INTEGER
		);`)
	if err != nil {
		return rollback(fmt.Errorf("failed to create migration_lock table: %w", err))
	}

	_, err = tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO migration_lock (id, owner, locked, lock_expires)
		VALUES (0, NULL, 0, NULL)
	`)
	if err != nil {
		return rollback(fmt.Errorf("failed to initialize migration lock row: %w", err))
	}

	ownerId := time.Now().UnixNano()

	now := time.Now().Unix()
	lockExpires := now + (5 * 60)
	res, err := tx.ExecContext(ctx, "UPDATE migration_lock SET locked = 1, lock_expires = ?, owner = ? WHERE id = 0 AND (locked = 0 OR lock_expires < ?)", lockExpires, ownerId, now)
	if err != nil {
		return rollback(fmt.Errorf("failed to acquire migration lock: %w", err))
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return rollback(fmt.Errorf("failed to check lock acquisition: %w", err))
	}
	if rows == 0 {
		return rollback(fmt.Errorf("another migration process is already running"))
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lock acquisition: %w", err)
	}

	return &Lock{
		id:   ownerId,
		conn: c.conn,
	}, nil
}

func (c *Connection) Close() error {
	swapped := c.closed.CompareAndSwap(false, true)
	if !swapped {
		return nil
	}
	if err := c.conn.Close(); err != nil {
		return err
	}
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.connections--
	return nil
}

func (c *Connection) AppliedMigrationStore() db.AppliedMigrationStore {
	return &AppliedMigrationStore{
		conn: c.conn,
	}
}

func (c *Connection) Exec(ctx context.Context, query string, args ...any) (err error) {
	if c.closed.Load() {
		return fmt.Errorf("connection is closed")
	}
	_, err = c.conn.ExecContext(ctx, query, args...)
	return err
}

func (c *Connection) BeginTrx(ctx context.Context) (trx db.Transaction, err error) {
	if c.closed.Load() {
		return nil, fmt.Errorf("connection is closed")
	}
	if ok := c.trxInProgress.CompareAndSwap(false, true); !ok {
		return nil, fmt.Errorf("transaction already in progress")
	}

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Transaction{
		tx:   tx,
		conn: c,
	}, nil
}
