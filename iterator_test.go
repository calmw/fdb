package fdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOption
	db, err := Open(opts)
	t.Log(err)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestIterator_One_Value(t *testing.T) {
	opts := DefaultOption
	db, _ := Open(opts)
	err := db.Put([]byte("hello"), []byte("world"))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
	t.Log(string(iterator.Key()))
	val, err := iterator.Value()
	t.Log(string(val))
}

func TestIterator_Key(t *testing.T) {

}

func TestIterator_Next(t *testing.T) {

}

func TestIterator_Rewind(t *testing.T) {

}

func TestIterator_Seek(t *testing.T) {

}

func TestIterator_Valid(t *testing.T) {

}

func TestIterator_Value(t *testing.T) {

}

func TestIterator_skipToNext(t *testing.T) {

}
