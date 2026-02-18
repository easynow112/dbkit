package db_test

import (
	"context"
	"sync"
	"testing"

	"github.com/easynow112/dbkit/db"
)

func TestLock(t *testing.T) {
	driverCases := getDriverCases()
	for _, driverCase := range driverCases {
		t.Run(driverCase.config.Driver, func(t *testing.T) {

			t.Run("locks are exclusive within a connection pool", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn1 := acquireConnection(t, pool)
				conn2 := acquireConnection(t, pool)
				acquireLock(t, conn1)
				lock2, err := conn2.TryAcquireLock(ctx)
				if err == nil {
					t.Cleanup(func() { lock2.Release(context.Background()) })
					t.Fatalf("expected attempt to aquire held lock to fail: %v", err)
				}
			})

			t.Run("locks are exclusive across connection pools", func(t *testing.T) {
				ctx := t.Context()
				pool1 := initDB(t, driverCase)
				pool2 := initDB(t, driverCase)
				conn1 := acquireConnection(t, pool1)
				conn2 := acquireConnection(t, pool2)
				acquireLock(t, conn1)
				lock2, err := conn2.TryAcquireLock(ctx)
				if err == nil {
					t.Cleanup(func() { lock2.Release(context.Background()) })
					t.Fatalf("expected attempt to aquire held lock to fail: %v", err)
				}
			})

			t.Run("lock exclusivity is respected across goroutines", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				wg := sync.WaitGroup{}
				routines := 4
				connections := make([]db.Connection, routines)
				for i := range routines {
					conn, err := pool.AcquireConnection(ctx)
					if err != nil {
						t.Fatalf("failed to acquire connection: %v", err)
					}
					connections[i] = conn
					t.Cleanup(func() { conn.Close() })
				}
				lockChan := make(chan db.Lock, routines)
				for i := range connections {
					wg.Add(1)
					go func(conn db.Connection) {
						defer wg.Done()
						lock, _ := conn.TryAcquireLock(ctx)
						lockChan <- lock
					}(connections[i])
				}
				wg.Wait()
				close(lockChan)
				locksAcquired := 0
				for lock := range lockChan {
					if lock != nil {
						locksAcquired++
						t.Cleanup(func() { lock.Release(context.Background()) })
					}
				}
				if locksAcquired != 1 {
					t.Fatalf("expected 1 lock to be acquired, got %d", locksAcquired)
				}
			})

			t.Run("locks exhibit reentrant behavior", func(t *testing.T) {
				ctx := t.Context()
				pool := initDB(t, driverCase)
				conn1 := acquireConnection(t, pool)
				conn2 := acquireConnection(t, pool)
				iterations := 4
				var lock1 db.Lock
				var err error
				for range iterations {
					lock1, err = conn1.TryAcquireLock(ctx)
					if err != nil {
						t.Fatalf("failed to acquire lock: %v", err)
					} else {
						t.Cleanup(func() { lock1.Release(context.Background()) })
					}
				}
				for i := range iterations {
					if err := lock1.Release(ctx); err != nil {
						t.Fatalf("lock release failed: %v", err)
					}
					lock2, err := conn2.TryAcquireLock(ctx)
					if err == nil {
						t.Cleanup(func() { lock2.Release(context.Background()) })
					}
					if i == iterations-1 {
						if err != nil {
							t.Fatalf("failed to acquire lock: %v", err)
						}
					} else {
						if err == nil {
							t.Fatalf("expected attempt to aquire held lock to fail")
						}
					}
				}
			})

		})
	}
}
