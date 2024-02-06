package fdb

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 数据文件的大小
	SyncWrite    bool   // 每次写入是否持久化
	IndexType    IndexType
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的 Key，默认为空
	Reverse bool   // 是否反向遍历，默认 false 是正向
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	MaxBatchNum int  // 一个批次中最大的数据量
	SyncWrites  bool // 提交时是否Sync持久化
}

type IndexType = int8

const (
	IndexTypeBtree IndexType = iota + 1 // Btree索引
	IndexTypeART                        //自适应基础树索引
)

var DefaultOption = Options{
	DirPath:      "./fdb",
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    IndexTypeBtree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}