package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func validateDirPath(path string) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("path is not absolute: %s", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("directory does not exist: %s", path)
		}
		return fmt.Errorf("failed to stat path: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}
	return nil
}
