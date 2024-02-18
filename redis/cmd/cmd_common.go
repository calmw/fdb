package main

import (
	"errors"
	"github.com/calmw/fdb"
	"github.com/calmw/fdb/redis"
	"github.com/tidwall/redcon"
)

// type redisDataType = byte
//
//const (
//	String redisDataType = iota
//	Hash
//	Set
//	List
//	ZSet
//)

var dataTypeMap = map[byte]string{
	redis.String: "string",
	redis.Hash:   "hash",
	redis.Set:    "set",
	redis.List:   "list",
	redis.ZSet:   "zset",
}

func Del(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("del")
	}

	var ok = 0
	key := args[0]
	_, err := cli.db.Get(key)
	if err != nil {
		if errors.Is(err, fdb.ErrKeyNotFound) {
			return redcon.SimpleInt(ok), nil
		} else {
			return nil, err
		}
	}

	err = cli.db.Del(key)
	if err != nil {
		return nil, err
	}
	ok = 1

	return redcon.SimpleInt(ok), nil
}

func Type(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("type")
	}

	key := args[0]
	dataType, err := cli.db.Type(key)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleString(dataTypeMap[dataType]), nil
}
