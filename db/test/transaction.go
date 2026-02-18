package test

import (
	"context"
	"fmt"
)

type Transaction struct {
	conn *Connection
}

func (trx *Transaction) Commit(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if ok := trx.conn.trxInProg.CompareAndSwap(true, false); !ok {
		return fmt.Errorf("no transaction is in progress")
	}
	return nil
}

func (trx *Transaction) Rollback(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if ok := trx.conn.trxInProg.CompareAndSwap(true, false); !ok {
		return fmt.Errorf("no transaction is in progress")
	}
	return nil
}

func (trx *Transaction) Exec(ctx context.Context, query string, args ...any) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}
