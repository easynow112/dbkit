package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type Lock struct {
	id   int64
	conn *sql.Conn
}

func (lock *Lock) Release(ctx context.Context) error {
	tx, err := lock.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	rollback := func(e error) error {
		_ = tx.Rollback()
		return e
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE migration_lock
		SET locked = 0,
		    lock_expires = NULL,
			owner = NULL
		WHERE id = 0 AND locked = 1 AND owner = ?
	`, lock.id)
	if err != nil {
		return rollback(fmt.Errorf("failed to release migration lock: %w", err))
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return rollback(fmt.Errorf("failed to check lock release: %w", err))
	}
	if rows != 1 {
		return rollback(fmt.Errorf("lock was not held or already released"))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit lock release: %w", err)
	}
	return nil
}
