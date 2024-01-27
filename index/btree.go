package index

import (
	"fdb/data"
	"github.com/google/btree"
	"sync"
)

// Btree Btree索引，封装了Google的btree
// github.com/google/btree
type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32), // 参数为控制叶子节点的数量
		lock: &sync.RWMutex{},
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeIterm := bt.tree.Get(it)
	if btreeIterm == nil {
		return nil
	}
	return btreeIterm.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldIterm := bt.tree.Delete(it)
	if oldIterm == nil {
		return false
	}
	return true
}
