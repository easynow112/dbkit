package migrations

import (
	"context"
	"fmt"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
	"github.com/easynow112/dbkit/source"
)

func Run(ctx context.Context, up bool, steps *int, cfg *config.Config, sourceStoreFactory source.StoreFactory, dbFactory db.DBFactory) error {

	upStore, err := sourceStoreFactory(ctx, cfg, cfg.Active.Source.Migrations.Up)
	if err != nil {
		return err
	}

	downStore, err := sourceStoreFactory(ctx, cfg, cfg.Active.Source.Migrations.Down)
	if err != nil {
		return err
	}

	sourceStore, err := newMigrationSourceStore(ctx, upStore, downStore)
	if err != nil {
		return err
	}

	db, err := dbFactory(ctx, cfg, cfg.Active.Database)
	if err != nil {
		return fmt.Errorf("Failed to load db driver.\n%v", err)
	}
	defer db.Close()

	conn, err := db.AcquireConnection(ctx)
	if err != nil {
		return fmt.Errorf("Failed to aquire db connection: %v", err)
	}
	defer conn.Close()

	lock, err := conn.TryAcquireLock(ctx)
	if err != nil {
		return fmt.Errorf("Failed to acquire lock: %v", err)
	}
	defer lock.Release(ctx)

	appliedStore := conn.AppliedMigrationStore()

	err = appliedStore.EnsureSchema(ctx)
	if err != nil {
		return fmt.Errorf("Failed to ensure applied migration schema exists: %v", err)
	}

	appliedMigrations, err := appliedStore.List(ctx)
	if err != nil {
		return fmt.Errorf("Failed to list applied migrations: %v", err)
	}

	err = ensureCleanState(appliedMigrations)
	if err != nil {
		return fmt.Errorf("Corrupted db state: %v", err)
	}

	pending, err := sourceStore.GetPending(ctx, appliedMigrations, up)
	if err != nil {
		return fmt.Errorf("Failed to get pending migrations: %v", err)
	}

	if len(pending) == 0 {
		fmt.Printf("✅  No pending %s migrations\n", direction(up))
		return nil
	}
	for i, pendingSource := range pending {
		if steps != nil && *steps <= i {
			return nil
		}

		upContents, downContents, checksum, err := pendingSource.contents(ctx)
		if err != nil {
			return err
		}
		var contents string
		if up {
			contents = upContents
		} else {
			contents = downContents
		}

		err = startMigration(ctx, pendingSource.id, checksum, appliedStore, up)
		if err != nil {
			return err
		}
		err = execMigration(ctx, pendingSource.id, contents, conn, up)
		if err != nil {
			return err
		}
		err = finishMigration(ctx, pendingSource.id, appliedStore, up)
		if err != nil {
			return err
		}
	}
	return nil
}

func startMigration(ctx context.Context, id string, checksum string, store db.AppliedMigrationStore, up bool) error {
	if up {
		if err := store.RecordStarted(ctx, id, checksum); err != nil {
			return fmt.Errorf("Failed to record migration %s start: %v", id, err)
		}
	} else {
		if err := store.RecordRollbackStarted(ctx, id); err != nil {
			return fmt.Errorf("Failed to record migration %s rollback start: %v", id, err)
		}
	}
	return nil
}

func execMigration(ctx context.Context, id string, contents string, conn db.Connection, up bool) error {
	if err := conn.Exec(ctx, contents); err != nil {
		return fmt.Errorf("Failed to execute %s migration %s: %v", direction(up), id, err)
	}
	return nil
}

func finishMigration(ctx context.Context, id string, store db.AppliedMigrationStore, up bool) error {
	if up {
		if err := store.RecordFinished(ctx, id); err != nil {
			return fmt.Errorf("Failed to record migration %s finish: %v", id, err)
		}
		fmt.Printf("⬆️  Up migration %s ran successfully\n", id)
	} else {
		if err := store.Remove(ctx, id); err != nil {
			return fmt.Errorf("Failed to record migration %s rollback finish: %v", id, err)
		}
		fmt.Printf("⬇️  Down migration %s ran successfully\n", id)
	}
	return nil
}

func ensureCleanState(appliedMigrations []db.AppliedMigration) error {
	for _, migration := range appliedMigrations {
		if migration.FinishedAt == nil {
			return fmt.Errorf("%s migration was started but did not successfully complete", migration.Id)
		} else if migration.RollbackStartedAt != nil {
			return fmt.Errorf("A rollback was started for migration %s but did not successfully complete", migration.Id)
		}
	}
	return nil
}

func direction(up bool) string {
	if up {
		return "up"
	}
	return "down"
}
