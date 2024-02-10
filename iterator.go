package fdb

import (
	"bytes"
	"github.com/calmw/fdb/index"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIter.Next() // 跳转到下一项
	it.skipToNext()     // 如果带前缀，需要判断上一步已经跳转到的项，是否带有指定前缀，没有的话，跳转到下一个带有该前缀的项
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 如果带前缀，需要跳转到下一个带有该前缀的项
func (it *Iterator) skipToNext() {
	perfixLen := len(it.options.Prefix)
	if perfixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if perfixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:perfixLen]) == 0 {
			break
		}
	}
}
