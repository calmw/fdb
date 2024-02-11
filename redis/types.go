package redis

import (
	"errors"
	"github.com/calmw/fdb"
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
