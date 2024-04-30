package server

import (
	fdbRedis "github.com/calmw/fdb/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

/// TODO 增加多数据库，增加密码验证

type FdbServer struct {
	Dbs    map[int]*fdbRedis.RedisDataStructure
	Server *redcon.Server
	Mu     sync.RWMutex
}

func (svr *FdbServer) Listen() {
	log.Println("fdb Server running,ready to Accept connections")
	err := svr.Server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func (svr *FdbServer) Accept(conn redcon.Conn) bool {
	cli := new(FdbClient)
	svr.Mu.Lock()
	defer svr.Mu.Unlock()
	cli.Server = svr
	cli.DB = svr.Dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *FdbServer) Close(conn redcon.Conn, err error) {
	for _, db := range svr.Dbs {
		_ = db.Close()
	}
	_ = svr.Server.Close()
}
