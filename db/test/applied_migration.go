package test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/easynow112/dbkit/db"
)

type AppliedMigrationStore struct {
	mu   sync.Mutex
	rows map[string]*db.AppliedMigration
}

func (store *AppliedMigrationStore) EnsureSchema(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (store *AppliedMigrationStore) List(ctx context.Context) ([]db.AppliedMigration, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	results := make([]db.AppliedMigration, 0, len(store.rows))
	for _, row := range store.rows {
		results = append(results, *row)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].StartedAt.Before(results[j].StartedAt)
	})
	return results, nil
}

func (store *AppliedMigrationStore) Remove(ctx context.Context, id string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.rows[id]; !ok {
		return fmt.Errorf("migration %s not found", id)
	}
	delete(store.rows, id)
	return nil
}

func (store *AppliedMigrationStore) RecordStarted(ctx context.Context, id string, checksum string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.rows[id]; ok {
		return fmt.Errorf("migration %s already exists", id)
	}
	store.rows[id] = &db.AppliedMigration{
		Id:                id,
		Checksum:          checksum,
		StartedAt:         time.Now(),
		FinishedAt:        nil,
		RollbackStartedAt: nil,
	}
	return nil
}

func (store *AppliedMigrationStore) RecordFinished(ctx context.Context, id string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.rows[id]; !ok {
		return fmt.Errorf("migration %s not found", id)
	}
	now := time.Now()
	store.rows[id].FinishedAt = &now
	return nil
}

func (store *AppliedMigrationStore) RecordRollbackStarted(ctx context.Context, id string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.rows[id]; !ok {
		return fmt.Errorf("migration %s not found", id)
	}
	now := time.Now()
	store.rows[id].RollbackStartedAt = &now
	return nil
}

func NewStore(rows map[string]*db.AppliedMigration) *AppliedMigrationStore {
	return &AppliedMigrationStore{
		rows: rows,
	}
}
