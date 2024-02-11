package redis

import (
	"encoding/binary"
	"errors"
	"github.com/calmw/fdb"
)

/// Hash 数据结构

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8) // 采用固定长度编码int64，占8位
	// key
	var index = 0
	copy(buf[:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
	// 先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, fdb.ErrKeyNotFound) {
		exist = false
	}

	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil

}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, fdb.ErrKeyNotFound) {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}
