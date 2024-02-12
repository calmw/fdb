package main

import "github.com/tidwall/redcon"

func lPush(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func rPush(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("rpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.RPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func lPop(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("lpop")
	}

	key := args[0]
	res, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}

	if len(res) > 0 {
		return res, nil
	}
	return nil, nil
}

func rPop(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("rpop")
	}

	key := args[0]
	res, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}

	if len(res) > 0 {
		return res, nil
	}
	return nil, nil
}
