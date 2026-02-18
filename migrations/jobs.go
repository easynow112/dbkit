package migrations

import (
	"context"
	"fmt"

	"github.com/easynow112/dbkit/source"
)

type createMigrationJob struct {
	src     source.Store
	label   string
	id      string
	content string
}

func (job *createMigrationJob) Run(ctx context.Context) error {
	return createMigration(ctx, job.src, job.label, job.id, job.content)
}

func (job *createMigrationJob) Rollback(ctx context.Context) error {
	return removeMigration(ctx, job.src, job.label, job.id)
}

func newCreateMigrationJob(src source.Store, label, id, content string) *createMigrationJob {
	return &createMigrationJob{
		src:     src,
		label:   label,
		id:      id,
		content: content,
	}
}

func createMigration(ctx context.Context, src source.Store, label, id, content string) error {
	err := src.Create(ctx, id, content)
	if err != nil {
		fmt.Printf("❌ error when creating %s migration:\n%v\n", label, err)
		return err
	}
	fmt.Printf("✅ %s migration created successfully: %s\n", label, id)
	return nil
}

func removeMigration(ctx context.Context, src source.Store, label, id string) error {
	fmt.Printf("attempting to remove %s migration...\n", label)
	err := src.Remove(ctx, id)
	if err != nil {
		fmt.Printf("❌ error when removing %s migration:\n%v\n", label, err)
		return err
	}
	fmt.Printf("%s migration removed successfully\n", label)
	return nil
}
