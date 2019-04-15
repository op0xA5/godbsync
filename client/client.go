package main

import (
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strings"
	"time"
	"util"
)

const ClientVersion = 10000

type Client struct {
	serverAddr string
	rootCAs    *x509.CertPool
	cert       tls.Certificate
	serverName string
	certHash   string
	timeout    *util.TimeoutConfig

	rpcClient *rpc.Client
}

func (c *Client) PrepareTLS() error {
	log.Printf("info: read parameters from db")

	timeoutStr, err := DB.GetValue(ValueTimeoutConfig)
	if err != nil {
		return fmt.Errorf("DB.GetValues '%s': %v", ValueTimeoutConfig, err)
	}
	c.timeout, err = util.ParseTimeoutConfig(timeoutStr)
	if err != nil {
		return fmt.Errorf("parse '%s': %v", ValueTimeoutConfig, err)
	}

	serverAddr, err := DB.RequireValue(ValueServerAddr)
	if err != nil {
		return fmt.Errorf("DB.RequireValue '%s': %v", ValueServerAddr, err)
	}
	host, _, err := net.SplitHostPort(serverAddr)
	if err != nil {
		return fmt.Errorf("parse '%s': should be 'host:port' format", ValueServerAddr)
	}
	c.serverAddr = serverAddr

	serverName, err := DB.GetValue(ValueServerName)
	if err != nil {
		return fmt.Errorf("DB.GetValue '%s': %v", ValueServerName, err)
	}
	if serverName == "" {
		serverName = host
	}

	caStrList, err := DB.GetValues(ValueServerCA)
	if err != nil {
		return fmt.Errorf("DB.GetValues '%s': %v", ValueServerCA, err)
	}
	certStr, err := DB.RequireValue(ValueCert)
	if err != nil {
		return fmt.Errorf("DB.RequireValue '%s': %v", ValueCert, err)
	}
	keyStr, err := DB.RequireValue(ValueCertKey)
	if err != nil {
		return fmt.Errorf("DB.RequireValue '%s': %v", ValueCertKey, err)
	}
	pool := x509.NewCertPool()
	for i := range caStrList {
		err := appendCertsFromPEM(pool, []byte(caStrList[i].Value))
		if err != nil {
			return fmt.Errorf("load CA '%s': %v", caStrList[i].Name, err)
		}
	}

	cert, err := tls.X509KeyPair([]byte(certStr), []byte(keyStr))
	if err != nil {
		return fmt.Errorf("load client cert: %v", err)
	}

	c.rootCAs = pool
	c.cert = cert
	c.serverName = serverName

	h := sha1.New()
	for i := range c.cert.Certificate {
		h.Write(c.cert.Certificate[i])
	}
	c.certHash = hex.EncodeToString(h.Sum(nil))
	log.Printf("info: cert fingerprint: %s", c.certHash)
	DB.SetValue("cert_fingerprint", c.certHash)
	return nil
}

func (c *Client) ConnectServer() error {
	dialer := new(net.Dialer)
	dialer.Timeout, _ = c.timeout.Get("connect", DefaultConnectTimeout)
	dialer.DualStack = false

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{c.cert},
		RootCAs:      c.rootCAs,
		ServerName:   c.serverName,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		},
		MinVersion: tls.VersionTLS12,
	}
	tlsConfig.BuildNameToCertificate()
	conn, err := tls.DialWithDialer(dialer, "tcp", c.serverAddr, tlsConfig)
	if err != nil {
		return fmt.Errorf("tls.Dial '%s': %v", c.serverAddr, err)
	}
	tc := util.NewTimeoutConn(conn)
	tc.ReadTimeout, _ = c.timeout.Get("read", DefaultReadTimeout)
	tc.WriteTimeout, _ = c.timeout.Get("write", DefaultWriteTimeout)
	c.rpcClient = rpc.NewClient(tc)

	args := &ClientConnectArgs{ClientVerson: ClientVersion}
	args.ClientID, err = DB.GetValue(ValueServerName)
	if err != nil {
		log.Printf("info: error DB.GetValue '%s': %v", ValueServerName, err)
	}
	reply := new(ClientConnectReply)
	err = c.Call("client.Connect", args, reply)
	if reply.Message == "" {
		reply.Message = "(empty)"
	}
	if c.SeverConnected() {
		log.Printf("server: Client.Connect response, serverVersion='%d'", reply.ServerVersion)
		log.Printf("server: client.Connect message: %s", reply.Message)
		DB.SetValue("server_message", reply.Message)
	}
	if err != nil {
		c.rpcClient.Close()
		c.rpcClient = nil
		return fmt.Errorf("rpc:Client.Connect: %v", err)
	}

	return nil
}
func (c *Client) SeverConnected() bool {
	return c.rpcClient != nil
}
func (c *Client) ConnectNotify() error {
	addr, err := c.RPCGetValue("notify_server_addr")
	if err != nil {
		return err
	}

	log.Printf("info: notify server addr: %s", addr)

	serverName := c.serverName
	if pos := strings.IndexByte(addr, '<'); pos != -1 {
		serverName = strings.TrimSuffix(addr[pos+1:], ">")
		addr = addr[:pos]
	}

	dialer := new(net.Dialer)
	dialer.Timeout, _ = c.timeout.Get("connect", DefaultConnectTimeout)
	dialer.DualStack = false

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{c.cert},
		RootCAs:      c.rootCAs,
		ServerName:   serverName,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		},
		MinVersion: tls.VersionTLS12,
	}
	tlsConfig.BuildNameToCertificate()
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("tls.Dial '%s': %v", addr, err)
	}

	log.Printf("info: notify server connected")
	rpcServ := rpc.NewServer()
	rpcServ.RegisterName("client", new(RpcClient))
	rpcServ.RegisterName("db", new(RpcDB))
	tc := util.NewTimeoutConn(conn)
	tc.ReadTimeout, _ = c.timeout.Get("read", DefaultReadTimeout)
	tc.WriteTimeout, _ = c.timeout.Get("write", DefaultWriteTimeout)
	rpcServ.ServeConn(tc)
	return nil
}

var ErrRpcClientNotInitialized = errors.New("rpc client not initialized")

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	if c.rpcClient == nil {
		return ErrRpcClientNotInitialized
	}
	err := c.rpcClient.Call(serviceMethod, args, reply)

	if err == rpc.ErrShutdown {
		c.rpcClient.Close()
		c.rpcClient = nil
	}
	if netErr, ok := err.(net.Error); ok && (netErr.Timeout() || !netErr.Temporary()) {
		c.rpcClient.Close()
		c.rpcClient = nil
	}
	return err
}
func (c *Client) RPCGetValue(key string) (string, error) {
	var val string
	err := c.Call("client.GetValue", key, &val)
	return val, err
}

func (c *Client) SetupHeartbeat() error {
	interval, _ := c.timeout.Get("heartbeat", DefaultHeartbeatTimeout)
	tick := time.Tick(interval)
	for now := range tick {
		magic := now.UnixNano() ^ 0x55AA55AA55AA55AA
		err := c.Call("client.Ping", magic, &magic)
		if err != nil {
			return fmt.Errorf("client send heartbeat: %v", err)
		}
	}
	return nil
}

func appendCertsFromPEM(pool *x509.CertPool, pemCerts []byte) error {
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}

		pool.AddCert(cert)
	}

	return nil
}

const DefaultConnectTimeout = 60 * time.Second
const DefaultReadTimeout = 60 * time.Second
const DefaultWriteTimeout = 5 * time.Second
const DefaultHeartbeatTimeout = 25 * time.Second
