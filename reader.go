package boltdb

import (
	"errors"
	"bytes"

	bolt "github.com/coreos/bbolt"
)

var (
	errNoBucket = errors.New("boltdb: no bucket choosed")
)

// Reader handles read-only operations on a bolt DB.
type Reader struct {
	store  *Store
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

// Switch to a bucket.
func (r *Reader) Switch(bucket string) *Reader {
	r.bucket = r.tx.Bucket([]byte(bucket))

	return r
}

// Get read the given key from the bucket.
func (r *Reader) Get(key []byte) ([]byte, error) {
	var (
		rv []byte
	)

	if r.bucket == nil {
		return nil, errNoBucket
	}

	if bytes.Equal(key, nil){
		return nil, ErrInvalidKey
	}

	v := r.bucket.Get(key)
	if v != nil {
		// Value returned from Get() is only valid while the transaction is open, so a copy is required.
		rv = make([]byte, len(v))
		copy(rv, v)
	}

	return rv, nil
}

// Close the internal transaction.
func (r *Reader) Close() error {
	return r.tx.Rollback()
}

// ForEach
func (r *Reader) ForEach(fn func(k, v []byte) error) error {
	if r.bucket == nil {
		return errNoBucket
	}

	return r.bucket.ForEach(fn)
}
