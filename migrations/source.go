package migrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/easynow112/dbkit/db"
	"github.com/easynow112/dbkit/source"
)

type migrationSource struct {
	id   string
	up   *source.Source
	down *source.Source
}

func (source *migrationSource) contents(ctx context.Context) (up string, down string, checksum string, err error) {
	upContents, err := source.up.Contents(ctx)
	if err != nil {
		return "", "", "", err
	}
	downContents, err := source.down.Contents(ctx)
	if err != nil {
		return "", "", "", err
	}
	checksumBytes := sha256.Sum256([]byte(upContents + downContents))
	checksum = hex.EncodeToString(checksumBytes[:])
	return upContents, downContents, checksum, nil
}

func (source *migrationSource) validateApplication(ctx context.Context, applied db.AppliedMigration) error {
	_, _, checksum, err := source.contents(ctx)
	if err != nil {
		return err
	}
	if !(source.up.Id == applied.Id && source.down.Id == applied.Id) {
		return fmt.Errorf("applied migration id does not match source id: applied id = %s, up id = %s, down id = %s", applied.Id, source.up.Id, source.down.Id)
	}
	if checksum != applied.Checksum {
		return fmt.Errorf("migration source corruption: %s has been altered since it was last applied", applied.Id)
	}
	return nil
}

func newMigrationSource(upSource *source.Source, downSource *source.Source) (source *migrationSource, err error) {
	if upSource.Id != downSource.Id {
		return nil, fmt.Errorf("up/down source id mismatch: upSource.id = %s but downSource.id = %s", upSource.Id, downSource.Id)
	}
	return &migrationSource{
		id:   upSource.Id,
		up:   upSource,
		down: downSource,
	}, nil
}
