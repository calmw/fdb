package index

import (
	"fmt"
	"github.com/calmw/fdb/data"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var bptreeBucketName = []byte("fdb-index")

// BPlusTree B+ 树索引
// 主要封装了 go.etcd.io/bbolt,该库支持并发访问，所以不用加锁
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrite
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), os.ModePerm, opts)
	if err != nil {
		fmt.Println(err)
		panic("failed to open bptree")
	}
	// 创建对应的 bucket
	if err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(bptreeBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bptreeBucketName)
		oldValue = bucket.Get(key)
		err := bucket.Put(key, data.EncodeLogRecordPos(pos))
		return err
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bptreeBucketName)
		value := bucket.Get(key)
		if len(value) > 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}

	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bptreeBucketName)
		if oldValue = bucket.Get(key); len(oldValue) > 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete oldValue in bptree")
	}

	if len(oldValue) == 0 {
		return nil, false
	}

	return data.DecodeLogRecordPos(oldValue), true
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bptreeBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}

	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// Iterator 索引迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBpTreeIterator(bpt.tree, reverse)
}

// ART 索引迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool // 是否是反向遍历
	currKey   []byte
	currValue []byte
}

func newBpTreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}

	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(bptreeBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()

	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) > 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback() // 只读事务用Rollback，具体看库代码注释
}
