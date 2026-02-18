package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Lock struct {
	id      int
	pgxConn *pgxpool.Conn
}

func (lock *Lock) Release(ctx context.Context) error {
	_, err := lock.pgxConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lock.id)
	return err
}
