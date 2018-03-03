package boltdb

import (
	"errors"
	"time"

	bolt "github.com/coreos/bbolt"
)

var (
	ErrEmptyPath    = errors.New("boltdb: empty path")
	ErrInvalidKey = errors.New("Invalid Key")
)

// Store represents a general bolt db.
type Store struct {
	path string
	db   *bolt.DB
}

// NewStore create/open a bolt database.
func NewStore(path string) (*Store, error) {
	if path == "" {
		return nil, ErrEmptyPath
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return nil, err
	}

	return &Store{
		path: path,
		db:   db,
	}, nil
}

// Reader returns a reader.
func (st *Store) Reader() (*Reader, error) {
	tx, err := st.db.Begin(false)
	if err != nil {
		return nil, err
	}

	return &Reader{
		store: st,
		tx:    tx,
	}, nil
}

// Writer returns a writer.
func (st *Store) Writer() (*Writer, error) {
	tx, err := st.db.Begin(true)
	if err != nil {
		return nil, err
	}

	return &Writer{
		store: st,
		tx:    tx,
	}, nil
}
