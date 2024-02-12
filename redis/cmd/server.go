package main

import (
	"github.com/calmw/fdb"
	fdbRedis "github.com/calmw/fdb/redis"
	"github.com/tidwall/redcon"
	"log"
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
	fdbServer := &FdbServer{
		dbs:    make(map[int]*fdbRedis.RedisDataStructure),
		server: nil,
		mu:     sync.RWMutex{},
	}
	fdbServer.dbs[0] = redisDataStructure

	// 初始化redis服务器
	fdbServer.server = redcon.NewServer(serverAddr, execClientCommand, fdbServer.accept, fdbServer.close)
	fdbServer.listen()
}

func (svr *FdbServer) listen() {
	log.Println("fdb server running,ready to accept connections")
	_ = svr.server.ListenAndServe()
}

func (svr *FdbServer) accept(conn redcon.Conn) bool {
	cli := new(FdbClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *FdbServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
