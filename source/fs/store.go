package fs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/easynow112/dbkit/config"
	"github.com/easynow112/dbkit/source"
)

var validID = regexp.MustCompile(`^[a-z0-9_-]+$`)

type FSSourceStore struct {
	dir string
}

func (store *FSSourceStore) validate() (err error) {
	err = validateDirPath(store.dir)
	if err != nil {
		return err
	}
	return nil
}

func NewFSSourceStore(_ context.Context, driverCfg *config.DriverConfig, globalCfg *config.GlobalConfig) (source.Store, error) {
	if driverCfg == nil {
		return nil, fmt.Errorf("driver config is nil")
	}
	if driverCfg.Driver != "fs" {
		return nil, fmt.Errorf("invalid driver: %s", driverCfg.Driver)
	}
	if globalCfg == nil {
		return nil, fmt.Errorf("global config is nil")
	}
	fsConfig, err := newConfig(driverCfg)
	if err != nil {
		return nil, err
	}
	store := FSSourceStore{dir: filepath.Join(globalCfg.BaseDir, fsConfig.Dir)}
	if err := store.validate(); err != nil {
		return nil, err
	}
	return &store, nil
}

func (store *FSSourceStore) List(ctx context.Context) ([]*source.Source, error) {
	entries, err := os.ReadDir(store.dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}
	var sources []*source.Source
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fileName := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(fileName, ".sql") {
			continue
		}
		id := strings.TrimSuffix(fileName, ".sql")
		fullPath := filepath.Join(store.dir, fileName)
		sources = append(sources, &source.Source{
			Id:       id,
			Contents: sourceContents(fullPath),
		})
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Id < sources[j].Id
	})

	return sources, nil
}

func (store *FSSourceStore) Create(ctx context.Context, id string, contents string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return errors.New("id cannot be empty")
	}
	if !validID.MatchString(id) {
		return fmt.Errorf("invalid id (must contains only lowercase alphanumeric characters, underscores, and hyphens): %s", id)
	}
	tmpFileName := id + ".tmp"
	tmpFilePath := filepath.Join(store.dir, tmpFileName)
	filePath := filepath.Join(store.dir, id+".sql")
	file, err := os.OpenFile(tmpFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("could not create file %s: %w", tmpFileName, err)
	}
	defer file.Close()
	if _, err := file.WriteString(contents); err != nil {
		return fmt.Errorf("could not write to file %s: %w", tmpFileName, err)
	}
	if err := os.Rename(tmpFilePath, filePath); err != nil {
		return fmt.Errorf("could not rename file %s to %s: %w", tmpFileName, filePath, err)
	}
	return nil
}

func (store *FSSourceStore) Remove(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return errors.New("id cannot be empty")
	}
	fileName := id + ".sql"
	fullPath := filepath.Join(store.dir, fileName)
	if err := os.Remove(fullPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("file not found: %s", fileName)
		}
		return fmt.Errorf("could not remove file %s: %w", fileName, err)
	}
	return nil
}
