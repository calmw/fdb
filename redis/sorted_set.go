package redis

import (
	"encoding/binary"
	"errors"
	"github.com/calmw/fdb"
	"github.com/calmw/fdb/utils"
)

/// ZSet 数据结构

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+8+len(zk.member)+len(scoreBuf)+4) // int64采用固定长度编码，占8字节,磁盘采用非固定长度存储;最后四个字节为member长度

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version)) // 整型要使用LittleEndian
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member))) // member长度

	return buf
}

// 不存储score
func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+8+len(zk.member)) // int64采用固定长度编码，占8字节,磁盘采用非固定长度存储

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version)) // 整型要使用LittleEndian
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf
}

// ZAdd
// 存储内容:
// zk.encodeWithMember()=>score // key中不包含score信息,score存储在value中
// zk.encodeWithScore()=>nil , key中包含score信息，value存空;key中分别是version、score、member、member size,如果把前两者看作一体的话，他就可以排序，version是固定的，所以相当于按照score排了序
// member不存在就新增，
// member存在，score跟之前不同就更新
// member存在，score跟之前相同就直接返回
// redis命令：ZADD key score member [[score member] [score member] ...]
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造一个数据部分的 key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查询member是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, fdb.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, fdb.ErrKeyNotFound) {
		exist = false
	}

	if exist { // member的score已经存在
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(fdb.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	} else {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			score:   utils.FloatFromBytes(value),
			member:  member,
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

// ZScore 根据key、member拿到score的值
func (rds *RedisDataStructure) ZScore(key, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}

	if meta.size == 0 {
		return -1, nil
	}

	// 构造一个数据部分的 key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	// 查询member是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
