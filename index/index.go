package index

import (
	"bytes"
	"github.com/calmw/fdb/data"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos // 向索引中存储key对应的数据位置信息,key已存在就更新value并返回旧的value，否则返回nil
	Get(key []byte) *data.LogRecordPos                         // 根据key取出对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)              // 根据key,删除对应的索引位置信息，已存在就删除并返回旧的value，否则返回nil
	Size() int                                                 // 索引中存在多少条数据
	Iterator(reverse bool) Iterator                            // 索引迭代器
	Close() error                                              // 特别是B+树索引是需要关闭，它本身就是一个DB实例，其实btree和amt是不需要的
}

type IndexType = int8

const (
	// BtreeType Btree 索引
	BtreeType IndexType = iota + 1

	// ARTType ART 自适应基数树索引
	ARTType

	// BPTree B+ 树索引
	BPTree
)

// NewIndexer 根据类型初始化索引
// sync b plus tree类型索引配置是否持久化
func NewIndexer(indexType IndexType, dirPath string, sync bool) Indexer {
	switch indexType {
	case BtreeType:
		return NewBtree()
	case ARTType:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
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

// Iterator 索引迭代器
type Iterator interface {
	Rewind()                   // 重新回到迭代器的起点，即第一个数据
	Seek(key []byte)           // 根据传入的Key查找到第一个大于（或小于）等于目标key，根据这个key开始遍历
	Next()                     // 跳转到下一个Key
	Valid() bool               // 是否有效，即是否已经遍历完了所有的key，用于退出遍历
	Key() []byte               // 当前遍历位置的key数据
	Value() *data.LogRecordPos // 当前遍历位置的value数据
	Close()                    // 关闭迭代器，释放相应资源
}
