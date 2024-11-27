package cmd

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// pool for tcp conn, aka, net.Conn
type SockConnPool struct {
	// buffered channel for connection pooling
	c chan net.Conn

	// factory to create new connection
	f        func(endpoint Endpoint) (net.Conn, error)
	endpoint Endpoint
}

func (p *SockConnPool) Init(wg *sync.WaitGroup) (net.Conn, error) {
	defer wg.Done()

	select {
	case c := <-p.c:
		//log.Printf("Got connection from pool: %p", c)
		return c, nil

	default:
		//log.Printf("Creating new connection")
		c, err := p.f(p.endpoint)
		if c == nil || err != nil {
			log.Printf("Failed to create new connection: %v", err)
			return nil, err
		}

		p.Put(c)
		return c, nil
	}
}

// get one idle conn from pool, if pool empty, create a new one
func (p *SockConnPool) Get() (net.Conn, error) {
	for {
		select {
		case c := <-p.c:
			//log.Printf("Got connection from pool: %p", c)
			return c, nil

		case <-time.After(5 * time.Microsecond):
			//log.Printf("Waiting for idle connection from pool...")
			continue
		}
	}
}

// put conn into pool, if the pool full, close the conn instead
func (p *SockConnPool) Put(c net.Conn) {
	select {
	case p.c <- c:
		//log.Printf("Connection idle, joined pool: %p", c)

	default:
		//log.Printf("SockConnPool full: closing current connection: %p", c)
		c.Close()
	}
}

// close the pool, and close all conns in it
func (p *SockConnPool) Close() {
	close(p.c)
	for c := range p.c {
		//log.Printf("Closing connection: %p", c)
		c.Close()
	}
}

func changePort(host string, newPort string) string {
	parts := strings.Split(host, ":")
	if len(parts) != 2 {
		return host
	}
	parts[1] = newPort
	return strings.Join(parts, ":")
}

/* XXX */
func NewConn(endpoint Endpoint) (net.Conn, error) {
	portNum := 0
	tmpPort := extractNumberWithRegex(endpoint.Path)
	if tmpPort != -1 {
		portNum = BasePort + tmpPort
	} else {
		portNum = BasePort
	}

	addrPort := strconv.Itoa(portNum)

	newHost := changePort(endpoint.Host, addrPort)
	//fmt.Println("[INFO] NewConn: host=", newHost)

	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial("tcp", newHost)
		//if err != nil {
		//    log.Fatal(err, "Unable to initialize conn")
		//}
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	/* configure socket connectino priority */
	file, err := conn.(*net.TCPConn).File()
	defer file.Close()
	//err = syscall.SetsockoptInt(int(file.Fd()), syscall.IPPROTO_IP, syscall.IP_TOS, 0)
	err = syscall.SetsockoptInt(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_PRIORITY, 0)
	if err != nil {
		fmt.Println("Error setting socket options:", err)
		return conn, err
	}

	return conn, err
}

func NewSockConnPool(s int, f func(endpoint Endpoint) (net.Conn, error), endpoint Endpoint) *SockConnPool {
	pool := &SockConnPool{
		c:        make(chan net.Conn, s),
		f:        f,
		endpoint: endpoint,
	}

	var wg sync.WaitGroup
	for i := 0; i < s; i++ {
		wg.Add(1)
		go pool.Init(&wg)
	}

	wg.Wait()

	//fmt.Println("[INFO] socket conn pool was initialized..., endpoint=", endpoint)
	return pool
}

type storageSocketClient struct {
	p *SockConnPool
}

func (client *storageSocketClient) Close() error {
	client.p.Close()
	client.p = nil
	return nil

}
func newStorageSocketClient(endpoint Endpoint) *storageSocketClient {
	poolSize := runtime.NumCPU()

	pool := NewSockConnPool(poolSize, NewConn, endpoint)

	sockClient := &storageSocketClient{
		p: pool,
	}

	return sockClient
}

func (client *storageSocketClient) WriteRequest(conn net.Conn, req *Request) error {
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		fmt.Println("[ERROR] Failed to encode request", err)
		return err
	}

	if false {
		alignedBytes := make([]byte, MaxMsgSize)
		copy(alignedBytes, reqBytes)
		_, err = conn.Write(alignedBytes)
	} else {
		_, err = conn.Write(reqBytes)
	}

	if err != nil {
		//fmt.Println("[Error] Failed to write request to server:", err)
		return err
	}

	return nil
}

func (client *storageSocketClient) ReadResponse(conn net.Conn) (res RawIOResponse, err error) {
	var resBytes []byte
	if false {
		resBytes = make([]byte, MaxMsgSize)
	} else {
		resBytes = msgChunkPool.Get().([]byte)
		defer msgChunkPool.Put(resBytes)
	}

	_, err = conn.Read(resBytes)
	if err != nil {
		//fmt.Println("[ERROR] Failed to read from server:", err)
		return res, err
	}

	_, err = res.UnmarshalMsg(resBytes)
	if err != nil {
		fmt.Println("[ERROR] decoding Request:", err)
		return res, err
	}

	return res, err
}

func (client *storageSocketClient) WriteRead(req *Request) (res RawIOResponse) {
	if client == nil {
		log.Fatalf("FATAL: storageSocketClient is nil")
	}

	conn, err := client.p.Get()
	if conn == nil {
		fmt.Println("[ERROR] connection is nil...")
	}

	defer client.p.Put(conn)
	if err != nil {
		return res
	}

	err = client.WriteRequest(conn, req)
	if err != nil {
		return res
	}

	res, err = client.ReadResponse(conn)
	if err != nil {
		return res
	}

	return res
}

func (client *storageSocketClient) WriteReadFileSlab(req *Request) (res FileSlabResponse) {
	conn, err := client.p.Get()
	defer client.p.Put(conn)
	if err != nil {
		return res
	}

	err = client.WriteRequest(conn, req)
	if err != nil {
		return res
	}

	//fmt.Println("[INFO] WriteReadFileSlab req=", *req)
	resBytes := make([]byte, 512<<10)

	_, err = conn.Read(resBytes)
	if err != nil {
		fmt.Println("[ERROR] Failed to read from server:", err)
		return res
	}

	_, err = res.UnmarshalMsg(resBytes)
	if err != nil {
		fmt.Println("[ERROR] decoding Request:", err)
		return res
	}

	return res
}
