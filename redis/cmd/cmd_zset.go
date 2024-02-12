package main

import (
	"fmt"
	"github.com/calmw/fdb/utils"
	"github.com/shopspring/decimal"
	"github.com/tidwall/redcon"
)

func zAdd(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func zScore(cli *FdbClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("zscore")
	}

	key, member := args[0], args[1]
	res, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}

	score := decimal.NewFromFloat(res)

	return redcon.SimpleString(fmt.Sprintf(`"%s"`, score.String())), nil
}
