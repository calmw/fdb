package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO 内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化MMap IO manager
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// 用mmap主要用来数据库启动或重启时，加快文件读取，写和sync其实不需要
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// 用mmap主要用来数据库启动或重启时，加快文件读取，写和sync其实不需要
func (mmap *MMap) Write(b []byte) (int, error) {
	panic("not implemented")
}

// 用mmap主要用来数据库启动或重启时，加快文件读取，写和sync其实不需要
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
