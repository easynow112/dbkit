package source

import (
	"context"
)

type Source struct {
	Id       string
	Contents func(ctx context.Context) (string, error)
}

type Store interface {
	List(ctx context.Context) ([]*Source, error)
	Remove(ctx context.Context, id string) error
	Create(ctx context.Context, id string, contents string) error
}
