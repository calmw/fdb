package main

import (
	"github.com/tidwall/redcon"
	"strconv"
	"time"
)

func set(cli *FdbClient, args [][]byte) (interface{}, error) {
	argsLen := len(args)
	if argsLen != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	err := cli.db.Set(key, 0, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func setEx(cli *FdbClient, args [][]byte) (interface{}, error) {
	argsLen := len(args)
	if argsLen != 3 {
		return nil, newWrongNumberOfArgsError("setex")
	}

	var ttl time.Duration
	key, value := args[0], args[2]
	if argsLen == 3 {
		ttlInt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return nil, err
		}
		ttl = time.Second * time.Duration(ttlInt)
	}

	err := cli.db.Set(key, ttl, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *FdbClient, args [][]byte) (interface{}, error) {
	argsLen := len(args)
	if argsLen != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}

	if len(value) > 0 {
		return value, nil
	}

	return nil, nil
}
