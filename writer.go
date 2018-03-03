package boltdb

import (
	"bytes"

	"github.com/coreos/bbolt"
)

// Writer handles write operations on a bolt DB.
type Writer struct {
	store *Store
	tx    *bolt.Tx
}

// Bucket create a bucket.
func (w *Writer) Bucket(bucket string) (*bolt.Bucket, error) {
	b, err := w.tx.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return nil, err
	}

	return b, nil
}

//Put put key-value into db, can't be used in multiple goroutines.
func (w *Writer) Put(bucket string, key []byte, value []byte) error {
	b, err := w.Bucket(bucket)
	if err != nil {
		return err
	}

	if bytes.Equal(key, nil){
		return ErrInvalidKey
	}

	err = b.Put(key, value)
	if err != nil {
		return err
	}

	return nil
}

// Commit closes the internal transaction.
func (w *Writer) Commit() error {
	return w.tx.Commit()
}

// Rollback closes the transaction and ignores all previous updates.
func (w *Writer) Rollback() error {
	return w.tx.Rollback()
}
