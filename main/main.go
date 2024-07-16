package main

import (
	easyrpc "easy-rpc"
	"easy-rpc/codec"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

func startServer(addr chan string) {
	// pick a free port
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("Network error:", err)
	}
	log.Println("start rpc server on:", l.Addr())
	addr <- l.Addr().String()
	easyrpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	// 事实上，下面的代码就像一个简单的 rpc 客户端一样
	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)
	// 发送 options
	_ = json.NewEncoder(conn).Encode(easyrpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	// 发送请求 & 接收响应
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("easy-rpc req %d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
