package seeds

import (
	"context"
	"fmt"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
	"github.com/easynow112/dbkit/source"
)

func Run(ctx context.Context, cfg *config.Config, sourceStoreFactory source.StoreFactory, dbFactory db.DBFactory) error {

	store, err := sourceStoreFactory(ctx, cfg, cfg.Active.Source.Seeds)
	if err != nil {
		return err
	}
	seeds, err := store.List(ctx)
	if err != nil {
		return fmt.Errorf("Failed to list seeds from store.\n%v", err)
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

	for _, seed := range seeds {
		err = execSeed(ctx, seed, conn)
		if err != nil {
			return fmt.Errorf("Failed to execute seed %s: %v", seed.Id, err)
		} else {
			fmt.Printf("✅  Seed %s ran successfully\n", seed.Id)
		}
	}

	return nil
}

func execSeed(ctx context.Context, source *source.Source, conn db.Connection) error {
	contents, err := source.Contents(ctx)
	if err != nil {
		return err
	}
	trx, err := conn.BeginTrx(ctx)
	if err != nil {
		return fmt.Errorf("Failed to begin transaction: %v", err)
	}
	err = trx.Exec(ctx, contents)
	if err != nil {
		trx.Rollback(ctx)
	} else {
		err = trx.Commit(ctx)
	}
	return err
}
