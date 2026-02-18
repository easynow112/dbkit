package migrations

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/easynow112/dbkit/db"
	"github.com/easynow112/dbkit/source"
	"github.com/easynow112/dbkit/workers"
)

type migrationSourceStore struct {
	upStore   source.Store
	downStore source.Store
}

func (store *migrationSourceStore) validate(ctx context.Context) error {
	_, err := store.list(ctx)
	return err
}

func (store *migrationSourceStore) list(ctx context.Context) ([]*migrationSource, error) {
	upSources, err := store.upStore.List(ctx)
	if err != nil {
		return nil, err
	}

	downSources, err := store.downStore.List(ctx)
	if err != nil {
		return nil, err
	}

	migrationSources := make([]*migrationSource, 0, len(upSources))

	if len(upSources) != len(downSources) {
		return nil, fmt.Errorf("migration stores corrupted: up store contains %d source(s) and down store contains %d source(s)", len(upSources), len(downSources))
	}

	for i, upSource := range upSources {
		downSource := downSources[i]
		source, err := newMigrationSource(upSource, downSource)
		if err != nil {
			return nil, fmt.Errorf("migration source at index %d is corrupted: %v", i, err)
		}
		migrationSources = append(migrationSources, source)
	}

	return migrationSources, nil
}

func (sourceStore *migrationSourceStore) GetPending(ctx context.Context, appliedMigrations []db.AppliedMigration, up bool) (pending []*migrationSource, err error) {
	sources, err := sourceStore.list(ctx)
	if err != nil {
		return nil, err
	}

	for i, source := range sources {
		if i < len(appliedMigrations) {
			appliedMigration := appliedMigrations[i]
			if err := source.validateApplication(ctx, appliedMigration); err != nil {
				return nil, err
			}
			if !up {
				pending = append(pending, source)
			}
			continue
		}
		if up {
			pending = append(pending, source)
		}
	}
	if !up {
		slices.Reverse(pending)
	}
	return pending, nil
}

func (sourceStore *migrationSourceStore) Create(ctx context.Context, id string, content string) (err error) {
	createUpJob := newCreateMigrationJob(sourceStore.upStore, "up", id, content)
	createDownJob := newCreateMigrationJob(sourceStore.downStore, "down", id, content)

	rbCtx, cancelRb := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelRb()

	_, err = workers.RunJobsAtomically(ctx, rbCtx, []workers.ReversibleJob{createUpJob, createDownJob}, 2)
	return err
}

func newMigrationSourceStore(ctx context.Context, upStore source.Store, downStore source.Store) (*migrationSourceStore, error) {
	store := migrationSourceStore{
		upStore:   upStore,
		downStore: downStore,
	}
	err := store.validate(ctx)
	if err != nil {
		return nil, err
	}
	return &store, nil
}
