package index

import (
	"bytes"
	"fdb/data"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos // 向索引中存储key对应的数据位置信息
	Get(key []byte) *data.LogRecordPos                         // 根据key取出对应的索引位置信息
	Delete(key []byte) bool                                    // 根据key删除对应的索引位置的信息
}

type IndexType = int8

const (
	IndexTypeBtree IndexType = iota + 1 // Btree索引
	IndexTypeART                        //自适应基础树索引
)

// NewIndexer 根据类型初始化索引
func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case IndexTypeBtree:
		return NewBtree()
	case IndexTypeART:
		// TODO
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}
