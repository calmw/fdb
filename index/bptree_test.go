package index

import (
	"fdb/data"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir())
	defer func() {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), bptreeIndexFileName))
	}()
	tree := NewBPlusTree(path, true)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 16})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 19})
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir())
	defer func() {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), bptreeIndexFileName))
	}()
	tree := NewBPlusTree(path, true)
	val0 := tree.Get([]byte("acc"))
	t.Log(val0)

	t.Log(tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 12}))
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 16})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 19})

	val1 := tree.Get([]byte("acc"))
	t.Log(val1)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir())
	defer func() {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), bptreeIndexFileName))
	}()
	tree := NewBPlusTree(path, true)
	val0 := tree.Get([]byte("acc"))
	t.Log(val0)

	t.Log(tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 12}))
	val1 := tree.Get([]byte("acc"))
	t.Log(val1)
	t.Log(tree.Delete([]byte("aaa")))
	t.Log(tree.Delete([]byte("acc")))
	val2 := tree.Get([]byte("acc"))
	t.Log(val2)

}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir())
	defer func() {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), bptreeIndexFileName))
	}()
	tree := NewBPlusTree(path, true)
	t.Log(tree.Size())

	t.Log(tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 12}))
	t.Log(tree.Size())
	t.Log(tree.Delete([]byte("acc")))
	t.Log(tree.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir())
	defer func() {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), bptreeIndexFileName))
	}()

	tree := NewBPlusTree(path, true)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 1})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	tree.Put([]byte("bcc"), &data.LogRecordPos{Fid: 1, Offset: 3})
	tree.Put([]byte("cdc"), &data.LogRecordPos{Fid: 1, Offset: 4})
	tree.Put([]byte("zcb"), &data.LogRecordPos{Fid: 1, Offset: 5})

	iterator := tree.Iterator(true)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		t.Log(string(iterator.Key()), iterator.Value())
	}
}
