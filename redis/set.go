package redis

import (
	"encoding/binary"
	"errors"
	"github.com/calmw/fdb"
)

/// Set 数据结构

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4) // 采用固定长度编码int64，占8字节,最后4个字节为member长度

	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	encKey := sk.encode()
	if _, err = rds.db.Get(encKey); errors.Is(err, fdb.ErrKeyNotFound) {
		// 不存在的话，则更新
		wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, fdb.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, fdb.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()
	if _, err = rds.db.Get(encKey); errors.Is(err, fdb.ErrKeyNotFound) {
		return false, err
	}

	// 更新
	wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(encKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return true, err
}
