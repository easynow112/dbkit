package test

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/easynow112/dbkit/db"
)

type Connection struct {
	trxInProg atomic.Bool
	closed    atomic.Bool
	db        *DB
}

func (conn *Connection) ensureOpen() error {
	conn.db.mu.Lock()
	defer conn.db.mu.Unlock()
	if conn.closed.Load() {
		return fmt.Errorf("connection is closed")
	}
	if conn.db.closed {
		return fmt.Errorf("connection pool is closed")
	}
	return nil
}

func (conn *Connection) TryAcquireLock(ctx context.Context) (db.Lock, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if err := conn.ensureOpen(); err != nil {
		return nil, err
	}
	switch ls.owner {
	case nil:
		ls.owner = conn
		ls.count = 1
	case conn:
		ls.count++
	default:
		return nil, fmt.Errorf("lock already held")
	}
	return &Lock{
		state: &ls,
		conn:  conn,
	}, nil
}

func (conn *Connection) Close() error {
	swapped := conn.closed.CompareAndSwap(false, true)
	if !swapped {
		return nil
	}
	ls.mu.Lock()
	if ls.owner == conn {
		ls.owner = nil
		ls.count = 0
	}
	ls.mu.Unlock()
	conn.db.mu.Lock()
	if conn.db.connections > 0 {
		conn.db.connections--
	}
	conn.db.mu.Unlock()
	return nil
}

func (conn *Connection) AppliedMigrationStore() db.AppliedMigrationStore {
	return conn.db.store
}

func (conn *Connection) Exec(ctx context.Context, query string, args ...any) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := conn.ensureOpen(); err != nil {
		return err
	}
	if query == "INVALID_QUERY" {
		return fmt.Errorf("query is invalid")
	}
	return nil
}

func (conn *Connection) BeginTrx(ctx context.Context) (db.Transaction, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := conn.ensureOpen(); err != nil {
		return nil, err
	}
	if ok := conn.trxInProg.CompareAndSwap(false, true); !ok {
		return nil, fmt.Errorf("a transaction already in progress")
	}
	return &Transaction{
		conn: conn,
	}, nil
}
