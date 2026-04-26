package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/easynow112/dbkit/db"
)

type AppliedMigrationStore struct {
	conn *sql.Conn
}

type rowDto struct {
	Id                string
	Checksum          string
	StartedAt         int64
	FinishedAt        *int64
	RollbackStartedAt *int64
}

func (r rowDto) appliedMigration() db.AppliedMigration {
	var finishedAt *time.Time
	if r.FinishedAt != nil {
		t := time.Unix(*r.FinishedAt, 0)
		finishedAt = &t
	}
	var rollbackStartedAt *time.Time
	if r.RollbackStartedAt != nil {
		t := time.Unix(*r.RollbackStartedAt, 0)
		rollbackStartedAt = &t
	}
	return db.AppliedMigration{
		Id:                r.Id,
		Checksum:          r.Checksum,
		StartedAt:         time.Unix(r.StartedAt, 0),
		FinishedAt:        finishedAt,
		RollbackStartedAt: rollbackStartedAt,
	}
}

func (store *AppliedMigrationStore) EnsureSchema(ctx context.Context) error {
	_, err := store.conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS migrations (
			id TEXT PRIMARY KEY,
			checksum TEXT,
			started_at INTEGER NOT NULL,
			finished_at INTEGER,
			rollback_started_at INTEGER
		);
	`)
	return err
}

func (store *AppliedMigrationStore) List(ctx context.Context) ([]db.AppliedMigration, error) {
	rows, err := store.conn.QueryContext(ctx, `SELECT id, checksum, started_at, finished_at, rollback_started_at FROM migrations ORDER BY migrations.started_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]db.AppliedMigration, 0)
	for rows.Next() {
		var row rowDto
		if err := rows.Scan(
			&row.Id,
			&row.Checksum,
			&row.StartedAt,
			&row.FinishedAt,
			&row.RollbackStartedAt,
		); err != nil {
			return nil, err
		}
		results = append(results, row.appliedMigration())
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (store *AppliedMigrationStore) Remove(ctx context.Context, id string) error {
	res, err := store.conn.ExecContext(ctx, `
		DELETE FROM migrations
		WHERE id = ?
	`, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rows)
	}
	return nil
}

func (store *AppliedMigrationStore) RecordStarted(ctx context.Context, id string, checksum string) error {
	res, err := store.conn.ExecContext(ctx, `
			INSERT INTO migrations (id, checksum, started_at)
			VALUES (?, ?, unixepoch('now'))
		`, id, checksum)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rows)
	}
	return nil
}

func (store *AppliedMigrationStore) RecordFinished(ctx context.Context, id string) error {
	res, err := store.conn.ExecContext(ctx, `
		UPDATE migrations
		SET finished_at = unixepoch('now')
		WHERE id = ?
	`, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rows)
	}
	return nil
}

func (store *AppliedMigrationStore) RecordRollbackStarted(ctx context.Context, id string) error {
	res, err := store.conn.ExecContext(ctx, `
		UPDATE migrations
		SET rollback_started_at = unixepoch('now')
		WHERE id = ?
	`, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rows)
	}
	return nil
}
