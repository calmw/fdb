package server

import (
	"errors"
	"fmt"
	"github.com/calmw/fdb"
	fdbRedis "github.com/calmw/fdb/redis"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *FdbClient, args [][]byte) (interface{}, error)

var supportCommands = map[string]cmdHandler{
	"type":      Type,
	"del":       Del,
	"set":       set,
	"setex":     setEx,
	"get":       get,
	"hset":      hSet,
	"hget":      hGet,
	"hdel":      hDel,
	"sadd":      sAdd,
	"sismember": sIsMember,
	"srem":      sRem,
	"lpush":     lPush,
	"rpush":     rPush,
	"lpop":      lPop,
	"rpop":      rPop,
	"zadd":      zAdd,
	"zscore":    zScore,
}

type FdbClient struct {
	DB     *fdbRedis.RedisDataStructure
	Server *FdbServer
}

func ExecClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportCommands[command]
	if !ok {
		conn.WriteError(fmt.Sprintf("Err unsupported command:%s", command))
		return
	}

	client, _ := conn.Context().(*FdbClient)
	switch command {
	case "quite": // 关闭连接
		_ = conn.Close()
	case "ping": // 查看服务是否运行
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, fdb.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}
