package db_test

import (
	"context"
	"testing"

	"github.com/easynow112/dbkit/db"
)

func acquireConnection(t *testing.T, db db.DB) db.Connection {
	t.Helper()
	ctx := t.Context()
	conn, err := db.AcquireConnection(ctx)
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	t.Cleanup(func() {
		err := conn.Close()
		if err != nil {
			t.Errorf("failed to close connection: %v", err)
		}
	})
	return conn
}

func acquireLock(t *testing.T, conn db.Connection) db.Lock {
	t.Helper()
	ctx := t.Context()
	lock, err := conn.TryAcquireLock(ctx)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	t.Cleanup(func() {
		err := lock.Release(context.Background())
		if err != nil {
			t.Errorf("failed to release lock: %v", err)
		}
	})
	return lock
}

func TestConnection(t *testing.T) {
	driverCases := getDriverCases()
	for _, driverCase := range driverCases {
		t.Run(driverCase.config.Driver, func(t *testing.T) {

			t.Run("closing an open connection succeeds", func(t *testing.T) {
				t.Parallel()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Close()
				if err != nil {
					t.Fatalf("failed to close connection: %v", err)
				}
			})

			t.Run("acquiring a lock succeeds", func(t *testing.T) {
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				acquireLock(t, conn)
			})

			t.Run("acquiring a lock respects context cancellation", func(t *testing.T) {
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				ctx := t.Context()
				cancelledCtx, cancel := context.WithCancel(ctx)
				cancel()
				lock, err := conn.TryAcquireLock(cancelledCtx)
				if err == nil {
					t.Cleanup(func() { lock.Release(context.Background()) })
					t.Fatalf("expected error when acquiring a lock with cancelled context")
				}
			})

			t.Run("acquiring a lock on a closed connection fails", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Close()
				if err != nil {
					t.Fatalf("failed to close connection: %v", err)
				}
				lock, err := conn.TryAcquireLock(ctx)
				if err == nil {
					t.Cleanup(func() { lock.Release(context.Background()) })
					t.Fatalf("expected error when acquiring a lock on closed connection")
				}
			})

			t.Run("connections return non-nil applied migration store", func(t *testing.T) {
				t.Parallel()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				store := conn.AppliedMigrationStore()
				if store == nil {
					t.Fatalf("expected non-nil applied migration store")
				}
			})

			t.Run("connections can execute valid queries", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Exec(ctx, "SELECT 1")
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			})

			t.Run("query execution respects context cancellation", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				cancelledCtx, cancel := context.WithCancel(ctx)
				cancel()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Exec(cancelledCtx, "SELECT 1")
				if err == nil {
					t.Fatalf("expected query to fail due to context cancellation")
				}
			})

			t.Run("executing an invalid query fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Exec(ctx, "INVALID_QUERY")
				if err == nil {
					t.Fatalf("expected an error due to invalid query")
				}
			})

			t.Run("executing a query on a closed connection fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Close()
				if err != nil {
					t.Fatalf("failed to close connection: %v", err)
				}
				err = conn.Exec(ctx, "SELECT 1")
				if err == nil {
					t.Fatalf("expected an error due to closed connection")
				}
			})

			t.Run("connections can begin transactions", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx, err := conn.BeginTrx(ctx)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				t.Cleanup(func() { trx.Rollback(context.Background()) })
			})

			t.Run("attempting to begin a transaction respects context cancellation", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				cancelledCtx, cancel := context.WithCancel(ctx)
				cancel()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				trx, err := conn.BeginTrx(cancelledCtx)
				if err == nil {
					t.Cleanup(func() { trx.Rollback(context.Background()) })
					t.Fatalf("expected transaction start to fail due to context cancellation")
				}
			})

			t.Run("attempting to begin a transaction on a closed connection fails", func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				err := conn.Close()
				if err != nil {
					t.Fatalf("failed to close connection: %v", err)
				}
				trx, err := conn.BeginTrx(ctx)
				if err == nil {
					t.Cleanup(func() { trx.Rollback(context.Background()) })
					t.Fatalf("expected an error due to closed connection")
				}
			})

			t.Run("connection closure is idempotent", func(t *testing.T) {
				t.Parallel()
				pool := initDB(t, driverCase)
				conn := acquireConnection(t, pool)
				if err := conn.Close(); err != nil {
					t.Fatalf("first Close failed: %v", err)
				}
				if err := conn.Close(); err != nil {
					t.Fatalf("second Close should not fail, got: %v", err)
				}
			})

		})
	}
}
