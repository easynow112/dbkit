package pg

import (
	"context"
	"fmt"

	"github.com/easynow112/dbkit/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AppliedMigrationStore struct {
	pgxConn *pgxpool.Conn
}

func (store *AppliedMigrationStore) EnsureSchema(ctx context.Context) error {
	_, err := store.pgxConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS migrations (
			id VARCHAR(255) PRIMARY KEY,
			checksum VARCHAR(255),
			started_at TIMESTAMPTZ NOT NULL,
			finished_at TIMESTAMPTZ,
			rollback_started_at TIMESTAMPTZ
		);
	`)
	return err
}

func (store *AppliedMigrationStore) List(ctx context.Context) ([]db.AppliedMigration, error) {
	rows, err := store.pgxConn.Query(ctx, `SELECT id, checksum, started_at, finished_at, rollback_started_at FROM migrations ORDER BY migrations.started_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]db.AppliedMigration, 0)
	for rows.Next() {
		var row db.AppliedMigration
		err = rows.Scan(
			&row.Id,
			&row.Checksum,
			&row.StartedAt,
			&row.FinishedAt,
			&row.RollbackStartedAt,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (store *AppliedMigrationStore) Remove(ctx context.Context, id string) error {
	cmdTag, err := store.pgxConn.Exec(ctx, `DELETE FROM migrations WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", cmdTag.RowsAffected())
	}
	return nil
}

func (store *AppliedMigrationStore) RecordStarted(ctx context.Context, id string, checksum string) error {
	cmdTag, err := store.pgxConn.Exec(ctx, `INSERT INTO migrations (id, checksum, started_at) VALUES ($1, $2, NOW())`, id, checksum)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", cmdTag.RowsAffected())
	}
	return nil
}

func (store *AppliedMigrationStore) RecordFinished(ctx context.Context, id string) error {
	cmdTag, err := store.pgxConn.Exec(ctx, `UPDATE migrations SET finished_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", cmdTag.RowsAffected())
	}
	return nil
}

func (store *AppliedMigrationStore) RecordRollbackStarted(ctx context.Context, id string) error {
	cmdTag, err := store.pgxConn.Exec(ctx, `UPDATE migrations SET rollback_started_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", cmdTag.RowsAffected())
	}
	return nil
}
