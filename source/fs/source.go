package fs

import (
	"context"
	"fmt"
	"os"
)

func sourceContents(path string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("could not read file %s: %w", path, err)
		}
		return string(data), nil
	}
}
