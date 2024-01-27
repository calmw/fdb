package fdb

import (
	"fdb/index"
)

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 数据文件的大小
	SyncWrite    bool   // 每次写入是否持久化
	IndexType    index.IndexType
}

var DefaultOption = Options{
	DirPath:      "./fdb",
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    index.IndexTypeBtree,
}
