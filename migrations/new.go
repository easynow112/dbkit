package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/source"
)

func New(ctx context.Context, name string, cfg *config.Config, sourceStoreFactory source.StoreFactory) error {
	id := fmt.Sprintf("%s_%s", time.Now().Format("2006-01-02_15-04-05"), name)

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

	if err := sourceStore.Create(ctx, id, ""); err != nil {
		return err
	}

	return nil
}
