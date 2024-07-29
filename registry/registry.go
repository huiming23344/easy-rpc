package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// EasyRegistry 是一个简单的注册中心，提供以下方法。
// 添加服务并接收心跳检查来保证其服务存活。
// 返回所有可用的服务并保持同步删除死掉的服务。
type EasyRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_easyrpc_/registry"
	defaultTimeout = time.Minute * 5
)

// New 创建一个带有超时设置的新的注册中心实例
func New(timeout time.Duration) *EasyRegistry {
	return &EasyRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultEasyRegistry = New(defaultTimeout)

// putServer 添加服务实例，如果服务已存在，更新 start
func (r *EasyRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		s.start = time.Now()
	}
}

// aliveServers 返回可用的服务列表，如果存在超时服务，删除
func (r *EasyRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// Runs at /_easyrpc_/registry
func (r *EasyRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET": // 返回所有可用的服务列表，通过自定义字段 X-Easyrpc-Servers
		// keep it simple, server is in req.Header
		w.Header().Set("X-Easyrpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST": // 添加服务实例或者发送心跳，通过自定义字段 X-Easyrpc-Server
		// keep it simple, server is in req.Header
		addr := req.Header.Get("X-Easyrpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP 注册了一个 HTTP handler 来给 EasyRegistry 在 registryPath 上
func (r *EasyRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultEasyRegistry.HandleHTTP(defaultPath)
}

// Heartbeat 每隔一段时间向注册中心发送心跳
// 这是给服务的帮助函数，来注册或者发送心跳
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 确保在注册移除前有足够的时间发送心跳
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil { // 每隔一段时间发送一次心跳
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Easyrpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
