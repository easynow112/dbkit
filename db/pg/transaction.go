package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Transaction struct {
	conn   *Connection
	pgxTrx pgx.Tx
}

func (trx *Transaction) Commit(ctx context.Context) error {
	if err := trx.pgxTrx.Commit(ctx); err != nil {
		return err
	}
	if ok := trx.conn.trxInProgress.CompareAndSwap(true, false); !ok {
		return fmt.Errorf("no transaction in progress")
	}
	return nil
}

func (trx *Transaction) Rollback(ctx context.Context) error {
	if err := trx.pgxTrx.Rollback(ctx); err != nil {
		return err
	}
	if ok := trx.conn.trxInProgress.CompareAndSwap(true, false); !ok {
		return fmt.Errorf("no transaction in progress")
	}
	return nil
}

func (trx *Transaction) Exec(ctx context.Context, query string, args ...any) error {
	_, err := trx.pgxTrx.Exec(ctx, query, args...)
	return err
}
