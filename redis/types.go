package redis

import (
	"encoding/binary"
	"errors"
	"github.com/calmw/fdb"
	"time"
)

var ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure Redis数据结构
type RedisDataStructure struct {
	db *fdb.DB
}

func NewRedisDataStructure(options fdb.Options) (*RedisDataStructure, error) {
	db, err := fdb.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

/// string 数据结构

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	// 编码value type+expire+payload
	buf := make([]byte, binary.MaxVarintLen64+1) // 用变长的方式编码expire+type
	buf[0] = String
	var index = 1
	var expire int64
	if ttl > 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储引擎接口进行写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	// 调用存储引擎接口读取数据
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := encValue[0]
	if dataType != String { // 检查类型
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && time.Now().UnixNano() > expire { // 检查过期时间
		return nil, nil
	}

	return encValue[index:], nil
}
