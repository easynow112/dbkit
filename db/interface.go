package db

import (
	"context"
	"time"
)

type DB interface {
	AcquireConnection(ctx context.Context) (Connection, error)
	Close() error
}

type Connection interface {
	TryAcquireLock(ctx context.Context) (Lock, error)
	Close() error
	AppliedMigrationStore() AppliedMigrationStore
	Exec(ctx context.Context, query string, args ...any) error
	BeginTrx(ctx context.Context) (Transaction, error)
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, query string, args ...any) error
}

type Lock interface {
	Release(ctx context.Context) error
}

type AppliedMigrationStore interface {
	EnsureSchema(ctx context.Context) error
	List(ctx context.Context) ([]AppliedMigration, error)
	Remove(ctx context.Context, id string) error
	RecordStarted(ctx context.Context, id string, checksum string) error
	RecordFinished(ctx context.Context, id string) error
	RecordRollbackStarted(ctx context.Context, id string) error
}

type AppliedMigration struct {
	Id                string
	Checksum          string
	StartedAt         time.Time
	FinishedAt        *time.Time
	RollbackStartedAt *time.Time
}
