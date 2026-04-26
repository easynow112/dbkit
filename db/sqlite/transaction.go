package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type Transaction struct {
	conn *Connection
	tx   *sql.Tx
}

func (trx *Transaction) Commit(ctx context.Context) error {
	if err := trx.tx.Commit(); err != nil {
		return err
	}
	if ok := trx.conn.trxInProgress.CompareAndSwap(true, false); !ok {
		return fmt.Errorf("no transaction in progress")
	}
	return nil
}

func (trx *Transaction) Rollback(ctx context.Context) error {
	if err := trx.tx.Rollback(); err != nil {
		return err
	}
	if ok := trx.conn.trxInProgress.CompareAndSwap(true, false); !ok {
		return fmt.Errorf("no transaction in progress")
	}
	return nil
}

func (trx *Transaction) Exec(ctx context.Context, query string, args ...any) error {
	_, err := trx.tx.ExecContext(ctx, query, args...)
	return err
}
