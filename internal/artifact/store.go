package artifact

import (
	"context"
	"io"
)

// Store is the interface for artifact persistence.
type Store interface {
	// Put uploads an object to the store.
	Put(ctx context.Context, key string, reader io.Reader, sizeBytes int64) error
	// Get downloads an object from the store.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Exists checks if an object exists.
	Exists(ctx context.Context, key string) (bool, error)
	// Delete removes an object.
	Delete(ctx context.Context, key string) error
	// List lists objects with the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)
}
