package server

import "github.com/tidwall/redcon"

func hSet(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.DB.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func hGet(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hget")
	}

	key, field := args[0], args[1]
	res, err := cli.DB.HGet(key, field)
	if err != nil {
		return nil, err
	}

	if len(res) > 0 {
		return res, nil
	}
	return nil, nil
}

func hDel(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hdel")
	}

	var ok = 0
	key, field := args[0], args[1]
	res, err := cli.DB.HDel(key, field)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}
