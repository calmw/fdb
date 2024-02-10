package index

import (
	"fdb/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewAdaptiveRadixTree(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	t.Log(art.Get([]byte("key-1")))
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	oldValue, ok := art.Delete([]byte("key-1"))
	assert.True(t, ok)
	t.Log(oldValue)
	oldValue, ok = art.Delete([]byte("key-2"))
	assert.False(t, ok)
	t.Log(oldValue)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	t.Log(pos)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 123})
	pos2 := art.Get([]byte("key-1"))
	t.Log(pos2)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {

	art := NewART()
	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 3})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 4})
	art.Put([]byte("gbhj"), &data.LogRecordPos{Fid: 1, Offset: 5})
	art.Put([]byte("xcdr"), &data.LogRecordPos{Fid: 1, Offset: 6})
	iterator := art.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		t.Log(string(iterator.Key()), iterator.Value())
	}
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	oldValue := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	oldValue = art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 13})
	t.Log(oldValue) // 旧的值
	pos := art.Get([]byte("key-1"))
	t.Log(pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	t.Log(art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	t.Log(art.Size())
}

func Test_artIterator_Close(t *testing.T) {

}

func Test_artIterator_Key(t *testing.T) {

}

func Test_artIterator_Next(t *testing.T) {

}

func Test_artIterator_Rewind(t *testing.T) {

}

func Test_artIterator_Seek(t *testing.T) {

}

func Test_artIterator_Valid(t *testing.T) {

}

func Test_artIterator_Value(t *testing.T) {

}

func Test_newArtIterator(t *testing.T) {

}
