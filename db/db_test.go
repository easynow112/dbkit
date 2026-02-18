package db_test

import (
	"context"
	"testing"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
)

func initDB(t *testing.T, driverCase driverCase) db.DB {
	t.Helper()
	ctx := t.Context()
	db, err := driverCase.factory(ctx, &driverCase.config, &config.GlobalConfig{})
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	t.Cleanup(func() {
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close pool: %v", err)
		}
	})
	return db
}

func TestDB(t *testing.T) {
	driverCases := getDriverCases()
	for _, driverCase := range driverCases {
		t.Run(driverCase.config.Driver, func(t *testing.T) {
			t.Parallel()

			t.Run("acquiring and closing connection succeeds", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn, err := pool.AcquireConnection(ctx)
				if err != nil {
					t.Fatalf("AcquireConnection failed: %v", err)
				}
				if err := conn.Close(); err != nil {
					t.Fatalf("connection Close failed: %v", err)
				}
			})

			t.Run("multiple connections can be acquired", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn1, err := pool.AcquireConnection(ctx)
				if err != nil {
					t.Fatalf("AcquireConnection 1 failed: %v", err)
				}
				t.Cleanup(func() { conn1.Close() })
				conn2, err := pool.AcquireConnection(ctx)
				if err != nil {
					t.Fatalf("AcquireConnection 2 failed: %v", err)
				}
				t.Cleanup(func() { conn2.Close() })
			})

			t.Run("opening a new pool respects cancelled context", func(t *testing.T) {
				cancelledCtx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := driverCase.factory(cancelledCtx, &driverCase.config, &config.GlobalConfig{})
				if err == nil {
					t.Fatalf("expected error when opening a pool with cancelled context")
				}
			})

			t.Run("connection acquisition respects cancelled context", func(t *testing.T) {
				cancelledCtx, cancel := context.WithCancel(context.Background())
				cancel()
				pool := initDB(t, driverCase)
				_, err := pool.AcquireConnection(cancelledCtx)
				if err == nil {
					t.Fatalf("expected error when acquiring with cancelled context")
				}
			})

			t.Run("attempting to close a pool with open connections fails", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn, err := pool.AcquireConnection(ctx)
				if err != nil {
					t.Fatalf("AcquireConnection failed: %v", err)
				}
				t.Cleanup(func() { conn.Close() })
				if err := pool.Close(); err == nil {
					t.Fatalf("expected error when closing pool with open connections")
				}
			})

			t.Run("internal counter remains correct when a connection is closed multiple times", func(t *testing.T) {
				pool := initDB(t, driverCase)
				conn1 := acquireConnection(t, pool)
				_ = acquireConnection(t, pool)
				conn1.Close() // Close single connection mutliple times
				conn1.Close()
				conn1.Close()
				if err := pool.Close(); err == nil {
					t.Fatalf("expected error when closing pool with open connections")
				}
			})

			t.Run("pool closure is idempotent", func(t *testing.T) {
				pool := initDB(t, driverCase)
				if err := pool.Close(); err != nil {
					t.Fatalf("first Close failed: %v", err)
				}
				if err := pool.Close(); err != nil {
					t.Fatalf("second Close should not fail, got: %v", err)
				}
			})

			t.Run("attempting to acquire a connection from a closed pool fails", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				if err := pool.Close(); err != nil {
					t.Fatalf("failed to close db pool: %v", err)
				}
				_, err := pool.AcquireConnection(ctx)
				if err == nil {
					t.Fatalf("expected error acquiring connection after Close, got nil")
				}
			})
		})
	}
}
