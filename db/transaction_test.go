package db_test

import (
	"context"
	"testing"

	"github.com/easynow112/dbkit/db"
)

func beginTrx(t *testing.T, conn db.Connection) db.Transaction {
	ctx := t.Context()
	trx, err := conn.BeginTrx(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	return trx
}

func TestTransaction(t *testing.T) {
	driverCases := getDriverCases()
	for _, driverCase := range driverCases {
		t.Run(driverCase.config.Driver, func(t *testing.T) {

			t.Run("committing a transaction succeeds", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx := beginTrx(t, conn)
				err := trx.Commit(ctx)
				if err != nil {
					t.Fatalf("failed to commit transaction: %v", err)
				}
			})

			t.Run("transaction rollback succeeds", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx := beginTrx(t, conn)
				err := trx.Rollback(ctx)
				if err != nil {
					t.Fatalf("failed to rollback transaction: %v", err)
				}
			})

			t.Run("attempting to commit a transaction after rollback fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx := beginTrx(t, conn)
				err := trx.Rollback(ctx)
				if err != nil {
					t.Fatalf("failed to rollback transaction: %v", err)
				}
				err = trx.Commit(ctx)
				if err == nil {
					t.Fatalf("expected transaction commit to fail after rollback: %v", err)
				}
			})

			t.Run("attempting to rollback a transaction after commit fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx := beginTrx(t, conn)
				err := trx.Commit(ctx)
				if err != nil {
					t.Fatalf("failed to commit transaction: %v", err)
				}
				err = trx.Rollback(ctx)
				if err == nil {
					t.Fatalf("expected transaction rollback to fail after commit: %v", err)
				}
			})

			t.Run("attempting to commit a transaction more than once fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx := beginTrx(t, conn)
				err := trx.Commit(ctx)
				if err != nil {
					t.Fatalf("failed to commit transaction: %v", err)
				}
				err = trx.Commit(ctx)
				if err == nil {
					t.Fatalf("expected transaction commit to fail: %v", err)
				}
			})

			t.Run("attempting to rollback a transaction more than once fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx := beginTrx(t, conn)
				err := trx.Rollback(ctx)
				if err != nil {
					t.Fatalf("failed to rollback transaction: %v", err)
				}
				err = trx.Rollback(ctx)
				if err == nil {
					t.Fatalf("expected transaction rollback to fail: %v", err)
				}
			})

			t.Run("attempting to start multiple transactions on the same connection fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx1 := beginTrx(t, conn)
				t.Cleanup(func() { trx1.Commit(context.Background()) })
				trx2, err := conn.BeginTrx(ctx)
				if err == nil {
					t.Cleanup(func() { trx2.Commit(context.Background()) })
					t.Fatalf("expected error: %v", err)
				}
			})

		})
	}
}
