package pg

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/easynow112/dbkit/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Connection struct {
	db            *DB
	pgxConn       *pgxpool.Conn
	closed        atomic.Bool
	trxInProgress atomic.Bool
}

func (conn *Connection) TryAcquireLock(ctx context.Context) (lock db.Lock, err error) {
	if conn.closed.Load() {
		return nil, fmt.Errorf("connection is closed")
	}
	defer func() {
		if r := recover(); r != nil {
			lock = nil
			err = fmt.Errorf("panic while acquiring lock: %v", r)
		}
	}()
	const id int = 3955278872
	var acquired bool
	err = conn.pgxConn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", id).Scan(&acquired)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, fmt.Errorf("another migration process is already running")
	}
	return &Lock{
		id:      id,
		pgxConn: conn.pgxConn,
	}, nil
}

func (conn *Connection) Close() error {
	swapped := conn.closed.CompareAndSwap(false, true)
	if !swapped {
		return nil
	}
	conn.pgxConn.Release()
	conn.db.mu.Lock()
	defer conn.db.mu.Unlock()
	conn.db.connections--
	return nil
}

func (conn *Connection) AppliedMigrationStore() db.AppliedMigrationStore {
	return &AppliedMigrationStore{
		pgxConn: conn.pgxConn,
	}
}

func (conn *Connection) Exec(ctx context.Context, query string, args ...any) (err error) {
	if conn.closed.Load() {
		return fmt.Errorf("connection is closed")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while executing query: %v", r)
		}
	}()
	_, err = conn.pgxConn.Exec(ctx, query, args...)
	return err
}

func (conn *Connection) BeginTrx(ctx context.Context) (trx db.Transaction, err error) {
	if conn.closed.Load() {
		return nil, fmt.Errorf("connection is closed")
	}
	if ok := conn.trxInProgress.CompareAndSwap(false, true); !ok {
		return nil, fmt.Errorf("transaction already in progress")
	}
	defer func() {
		if r := recover(); r != nil {
			trx = nil
			err = fmt.Errorf("panic while beginning transaction: %v", r)
		}
	}()
	pgTrx, err := conn.pgxConn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	return &Transaction{
		pgxTrx: pgTrx,
		conn:   conn,
	}, nil
}
