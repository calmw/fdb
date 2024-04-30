package main

import (
	"github.com/calmw/fdb"
	fdbRedis "github.com/calmw/fdb/redis"
	fdbRedisServer "github.com/calmw/fdb/redis/server"
	"github.com/tidwall/redcon"
	"sync"
)

const serverAddr = "0.0.0.0:6379"

type FdbServer struct {
	dbs    map[int]*fdbRedis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

// TODO 增加多数据库，增加密码验证
func main() {
	// 打开redis数据结构服务
	redisDataStructure, err := fdbRedis.NewRedisDataStructure(fdb.DefaultOption)
	if err != nil {
		panic(err)
	}

	// 初始化fdb server
	fdbServer := &fdbRedisServer.FdbServer{
		Dbs:    make(map[int]*fdbRedis.RedisDataStructure),
		Server: nil,
		Mu:     sync.RWMutex{},
	}
	fdbServer.Dbs[0] = redisDataStructure

	// 初始化redis服务器
	fdbServer.Server = redcon.NewServer(serverAddr, fdbRedisServer.ExecClientCommand, fdbServer.Accept, fdbServer.Close)
	fdbServer.Listen()
}
