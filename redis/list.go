package redis

import (
	"encoding/binary"
	"github.com/calmw/fdb"
)

/// List 数据结构

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8) // int64采用固定长度编码，占8字节

	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:index+8], lk.index)

	return buf
}

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) pushInner(key []byte, element []byte, isLeft bool) (uint32, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造一个数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	// 开始的时候，head和tail相等，LPush从中间减1开始，RPush从中间开始；当然RPush从中间减1开始，LPush从中间开始也可以
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	encKey := lk.encode()
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(encKey, element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	element, err = rds.db.Get(key)

	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// 构造一个数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	encKey := lk.encode()
	element, err := rds.db.Get(encKey)

	if err != nil {
		return nil, err
	}

	// 更新元数据和数据部分
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	///
	//if err = rds.db.Put(key, meta.encode()); err != nil {
	//	return nil, err
	//}
	// 将原来上面部分换成下面的，增加了删除元素的操作
	wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(encKey)
	if err = wb.Commit(); err != nil {
		return nil, err
	}
	///

	return element, nil
}
