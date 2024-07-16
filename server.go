package easy_rpc

import (
	"easy-rpc/codec"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x233333

// Option 编解码方式
type Option struct {
	MagicNumber int        // MagicNumber 标记这是一个 easy_rpc 请求
	CodecType   codec.Type // 客户端可以选择不同的 Codec 来编码 body
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server 代表了一个 RPC Server.
type Server struct{}

// NewServer 返回一个新 Server
func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是 *Server 的默认实例
var DefaultServer = NewServer()

// Accept 从 listener 接收连接并为传入的连接服务每一个请求
func (server *Server) Accept(lis *net.Listener) {
	for {
		conn, err := (*lis).Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept 从 listener 接收连接并为传入的连接服务每一个请求
func Accept(lis *net.Listener) { DefaultServer.Accept(lis) }

// ServeConn 来在单个连接上运行服务.
// ServeConn 会阻塞，直到客户端断开连接都会一直服务。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error:", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
	}
	server.serveCodec(f(conn))
}

// invalidRequest 是当发生错误时持有响应参数的占位符
var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex) // 确保发送一个完整的响应
	wg := new(sync.WaitGroup)  // 等待所有的请求都处理完成
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // 解析失败，无法恢复，所以关闭连接
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg)
	}
	wg.Wait()
	_ = cc.Close()
}

// request 存储了一个调用所有的信息
type request struct {
	h            *codec.Header // 请求头
	argv, replyv reflect.Value // 请求参数和返回参数
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	// TODO: 目前我们不知道请求参数
	// 第一天，只认为他是一个 string 就好了
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// TODO: 应该调用注册的 rpc 方法来获得正确的 replyv
	// 第一天，只打印 argv 然后发回一个 hello 消息就好了
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("reay-rpc resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}
