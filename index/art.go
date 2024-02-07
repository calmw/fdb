package index

import (
	"bytes"
	"fdb/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: &sync.RWMutex{},
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos) // If the key already in the tree then return oldValue, true and nil, false otherwise.
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)

	return deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
	//art.lock.RLock()
	//defer art.lock.RUnlock()
	//return art.Size()
	// 多个defer的执行顺序为“后进先出”；
	//defer、return、返回值三者的执行逻辑应该是：return最先执行，return负责将结果写入返回值中；接着defer开始执行一些收尾工作；最后函数携带当前返回值退出。
}

// Iterator 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.Lock()
	defer art.lock.Unlock()
	return newArtIterator(art.tree, reverse)
}

// ART 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历的位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key位置索引信息
}

func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValue := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	// 将所有数据存放到数组中
	tree.ForEach(saveValue)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (art *artIterator) Rewind() {
	art.currIndex = 0
}

func (art *artIterator) Seek(key []byte) {
	if art.reverse {
		art.currIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) <= 0
		})
	} else {
		art.currIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) >= 0
		})
	}
}

func (art *artIterator) Next() {
	art.currIndex++
}

func (art *artIterator) Valid() bool {
	return art.currIndex < len(art.values)
}

func (art *artIterator) Key() []byte {
	return art.values[art.currIndex].key
}

func (art *artIterator) Value() *data.LogRecordPos {
	return art.values[art.currIndex].pos
}

func (art *artIterator) Close() {
	art.values = nil
}
