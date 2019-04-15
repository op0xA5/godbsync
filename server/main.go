package main

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
	"util"
)

const ConfigFile = "config.json"
const ServerVersion = 10000

const DefaultReadTimeout = 60 * time.Second
const DefaultWriteTimeout = 5 * time.Second
const DefaultHeartbeatTimeout = 25 * time.Second

func main() {
	if !Config.IsFileExist(ConfigFile) {
		err := Config.Save(ConfigFile)
		if err != nil {
			log.Fatalf("FAILED create config file '%s': %v", ConfigFile, err)
			return
		}
		log.Printf("info: config file created, please check config and restart server")
		return
	}

	err := Config.Load(ConfigFile)
	if err != nil {
		log.Fatalf("FAILED load config file '%s': %v", ConfigFile, err)
		return
	}

	if Config.NotifyServerAddr == "" {
		log.Fatalf("FAILED config NotifyServerAddr not set")
		return
	}
	if Config.NotifyServerName == "" {
		log.Fatalf("FAILED config NotifyServerName not set")
		return
	}
	if Config.PushTimeout == "" {
		log.Fatalf("FAILED config PushTimeout not set")
		return
	}

	timeoutConfig, err := util.ParseTimeoutConfig(Config.Timeout)
	if err != nil {
		log.Fatalf("FAILED parse config Timeout: %v", err)
		return
	}

	flog, err := os.OpenFile(Config.Log, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		log.Fatalf("FAILED write log: %v", err)
		return
	}
	defer flog.Close()
	log.SetOutput(io.MultiWriter(os.Stderr, flog))
	log.Println("info: app started")

	SQL.Init(Config)
	dsn, err := readDSN(Config.DSNFile)
	if err != nil {
		log.Fatalf("FAILED load DSN: %v", err)
		return
	}
	err = DB.Open(dsn)
	if err != nil {
		log.Fatalf("FAILED Open database: %v", err)
		return
	}

	tlsConfig, err := NewTLSConfig(Config)
	if err != nil {
		log.Fatalf("FAILED config TLS: %v", err)
		return
	}

	l, err := tls.Listen("tcp", Config.Listen, tlsConfig.Clone())
	if err != nil {
		log.Fatalf("FAILED Create Server '%s': %v", Config.Listen, err)
		return
	}
	lHttp, err := net.Listen("tcp", Config.HttpListen)
	if err != nil {
		log.Fatalf("FAILED Create Server '%s': %v", Config.HttpListen, err)
		return
	}
	lNotify, err := tls.Listen("tcp", Config.NotifyListen, tlsConfig.Clone())
	if err != nil {
		log.Fatalf("FAILED Create Server '%s': %v", Config.NotifyListen, err)
		return
	}

	go startRPCServ(l, timeoutConfig)
	go startNotifyServ(lNotify, timeoutConfig)
	err = http.Serve(lHttp, http.DefaultServeMux)
	if err != nil {
		log.Fatalf("FAILED serve http '%s': %v", Config.HttpListen, err)
		return
	}
}

func startRPCServ(l net.Listener, timeout *util.TimeoutConfig) {
	rpcServ := rpc.NewServer()
	rpcServ.RegisterName("client", new(RpcClient))

	var servConn = func(conn io.ReadWriteCloser) {
		defer func() {
			atomic.AddInt64(&Stat.ConnectionRPC, -1)
			p := recover()
			if p != nil {
				log.Printf("error panic rpcServ.ServeConn: %v", p)
			}
		}()
		atomic.AddInt64(&Stat.ConnectionRPC, 1)
		rpcServ.ServeConn(conn)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Print("rpc.Serve: accept:", err.Error())
			return
		}
		tc := util.NewTimeoutConn(conn)
		tc.ReadTimeout, _ = timeout.Get("read", DefaultReadTimeout)
		tc.WriteTimeout, _ = timeout.Get("write", DefaultWriteTimeout)
		go servConn(tc)
	}
}

func startNotifyServ(l net.Listener, timeout *util.TimeoutConfig) {
	var handleConn = func(conn io.ReadWriteCloser) {
		defer func() {
			atomic.AddInt64(&Stat.ConnectionRPC, -1)
			p := recover()
			if p != nil {
				log.Printf("error panic handleNotifyConn: %v", p)
			}
		}()
		atomic.AddInt64(&Stat.ConnectionRPC, 1)
		handleNotifyConn(conn)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Print("rpc.Serve: accept:", err.Error())
			return
		}
		tc := util.NewTimeoutConn(conn)
		tc.ReadTimeout, _ = timeout.Get("read", DefaultReadTimeout)
		tc.WriteTimeout, _ = timeout.Get("write", DefaultWriteTimeout)
		go handleConn(tc)
	}
}
