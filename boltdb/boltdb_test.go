package boltdb

import (
	"testing"
)

var (
	store *Store
)

func TestNewStore(t *testing.T) {
	var (
		err error
	)

	store, err = NewStore("./user.db")

	if err != nil {
		t.Fatal(err)
	}
}

func TestRollback(t *testing.T) {
	var (
		err error
	)

	wr, err := store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	err = wr.Put("user", []byte("rollback"), []byte("a"))
	if err != nil {
		wr.Rollback()
		t.Fatal(err)
	}
	wr.Commit()

	writer, err := store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Put("user", []byte("rollback"), []byte("test"))
	if err != nil {
		writer.Rollback()
		t.Fatal(err)
	}

	err = writer.Put("user", []byte(""), []byte("test"))
	if err != nil {
		writer.Rollback()
	} else {
		writer.Commit()
	}

	reader, err := store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	m, err := reader.Switch("user").Get([]byte("rollback"))
	if err != nil || string(m) != "a" {
		t.Fatal(err)
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCommit(t *testing.T) {
	var (
		err error
	)

	w, err := store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	err = w.Put("user", []byte("commit"), []byte("a"))
	if err != nil {
		w.Rollback()
		t.Fatal(err)
	}

	err = w.Put("user", []byte("commit"), []byte("test"))
	if err != nil {
		w.Rollback()
		t.Fatal(err)
	} else {
		w.Commit()
	}

	reader, err := store.Reader()
	if err != nil {
		t.Fatal(err)
	}
	n, err := reader.Switch("user").Get([]byte("commit"))
	if err != nil || string(n) != "test" {
		t.Fatal(err)
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReader_Get(t *testing.T) {
	var (
		err error
	)

	wr, err := store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	err = wr.Put("user", []byte("get"), []byte("get"))
	if err != nil {
		wr.Rollback()
		t.Fatal(err)
	}
	wr.Commit()


	reader, err := store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	m, err := reader.Switch("user").Get([]byte("get"))
	if err != nil || string(m) != "get" {
		t.Fatal(err)
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}
