package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/easynow112/dbkit/apperrors"
	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/db"
	_ "github.com/easynow112/dbkit/db/pg"
	"github.com/easynow112/dbkit/migrations"
	"github.com/easynow112/dbkit/msg"
	"github.com/easynow112/dbkit/seeds"
	"github.com/easynow112/dbkit/source"
	_ "github.com/easynow112/dbkit/source/fs"
)

func main() {
	fmt.Println()
	err := run(os.Args, config.LoadConfig, source.NewStore, db.NewDB)
	if err != nil {
		var errInvalidArgs *apperrors.InvalidArgs
		if errors.As(err, &errInvalidArgs) {
			fmt.Println(errInvalidArgs.Error())
			fmt.Printf("Hint:\n%s\n", errInvalidArgs.Hint)
		} else {
			fmt.Println(err)
		}
		fmt.Println()
		os.Exit(1)
	}
	fmt.Println()
	os.Exit(0)
}

func run(args []string, configFactory config.ConfigFactory, sourceStoreFactory source.StoreFactory, dbFactory db.DBFactory) error {
	cfg, err := configFactory()
	if err != nil {
		return fmt.Errorf("Failed to load %s config:\n%v", config.FilePath, err)
	}

	if len(args) < 2 {
		return &apperrors.InvalidArgs{
			Args: args,
			Hint: msg.Usage,
		}
	}

	ctxSig, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(ctxSig, 30*time.Second)
	defer cancel()
	defer stop()

	switch args[1] {
	case "migrate":
		{
			if len(args) < 3 {
				return &apperrors.InvalidArgs{
					Args: args,
					Hint: msg.Usage,
				}
			}
			switch args[2] {
			case "new":
				return handleMigrateNew(ctx, args, cfg, sourceStoreFactory)
			case "up":
				return handleMigrateUp(ctx, args, cfg, sourceStoreFactory, dbFactory)
			case "down":
				return handleMigrateDown(ctx, args, cfg, sourceStoreFactory, dbFactory)
			}
		}
	case "seed":
		{
			if len(args) == 2 {
				return handleSeed(ctx, cfg, sourceStoreFactory, dbFactory)
			} else if len(args) == 4 && args[2] == "new" {
				return handleSeedNew(ctx, args, cfg, sourceStoreFactory)
			}
		}
	}
	return &apperrors.InvalidArgs{
		Args: args,
		Hint: msg.Usage,
	}
}

func handleMigrateNew(ctx context.Context, args []string, cfg *config.Config, sourceStoreFactory source.StoreFactory) error {
	if len(args) != 4 {
		return &apperrors.InvalidArgs{
			Args: args,
			Hint: msg.UsageMigrateNew,
		}
	}
	return migrations.New(ctx, args[3], cfg, sourceStoreFactory)
}

func handleMigrateUp(ctx context.Context, args []string, cfg *config.Config, sourceStoreFactory source.StoreFactory, dbFactory db.DBFactory) error {
	var steps *int = nil
	if len(args) > 4 {
		return &apperrors.InvalidArgs{
			Args: args,
			Hint: msg.UsageMigrateUp,
		}
	} else if len(args) == 4 {
		stepsInt, err := parseSteps(args[3])
		if err != nil {
			return &apperrors.InvalidArgs{
				Args: args,
				Hint: err.Error(),
			}
		} else {
			steps = &stepsInt
		}
	}
	return migrations.Run(ctx, true, steps, cfg, sourceStoreFactory, dbFactory)
}

func handleMigrateDown(ctx context.Context, args []string, cfg *config.Config, sourceStoreFactory source.StoreFactory, dbFactory db.DBFactory) error {
	var steps int = 1
	var err error
	if len(args) > 4 {
		return &apperrors.InvalidArgs{
			Args: args,
			Hint: msg.UsageMigrateDown,
		}
	} else if len(args) == 4 {
		steps, err = parseSteps(args[3])
		if err != nil {
			return &apperrors.InvalidArgs{
				Args: args,
				Hint: err.Error(),
			}
		}
	}
	return migrations.Run(ctx, false, &steps, cfg, sourceStoreFactory, dbFactory)
}

func handleSeedNew(ctx context.Context, args []string, cfg *config.Config, sourceStoreFactory source.StoreFactory) error {
	if len(args) != 4 {
		return &apperrors.InvalidArgs{
			Args: args,
			Hint: msg.UsageSeedNew,
		}
	}
	return seeds.New(ctx, args[3], cfg, sourceStoreFactory)
}

func handleSeed(ctx context.Context, cfg *config.Config, sourceStoreFactory source.StoreFactory, dbFactory db.DBFactory) error {
	return seeds.Run(ctx, cfg, sourceStoreFactory, dbFactory)
}

func parseSteps(input string) (int, error) {
	steps, err := strconv.Atoi(input)
	if err != nil {
		return 0, fmt.Errorf("Invalid input, '%s' could not be parsed as an integer:\n%v", input, err)
	} else if steps < 1 {
		return 0, fmt.Errorf("Invalid number of steps, expected a positive integer, received: %d", steps)
	}
	return steps, nil
}
