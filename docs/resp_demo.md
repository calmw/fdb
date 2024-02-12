#### RESP协议demo

``` go
package main

import (
	"bufio"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}

	// 向redis发送一个命令
	cmd := "set k-name some-value\r\n"
	_, _ = conn.Write([]byte(cmd))

	// 解析redis响应
	reader := bufio.NewReader(conn)
	res, err := reader.ReadString('\n')
	if err != nil {
		panic(err)
	}
	log.Println(res)
}

```