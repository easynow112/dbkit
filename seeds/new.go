package seeds

import (
	"context"
	"fmt"
	"time"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/source"
)

func New(ctx context.Context, name string, cfg *config.Config, sourceStoreFactory source.StoreFactory) error {

	id := fmt.Sprintf("%s_%s", time.Now().Format("2006-01-02_15-04-05"), name)

	store, err := sourceStoreFactory(ctx, cfg, cfg.Active.Source.Seeds)
	if err != nil {
		return err
	}

	if err := store.Create(ctx, id, ""); err != nil {
		return err
	}

	fmt.Printf("✅ seed created successfully: %s\n", id)

	return nil
}
