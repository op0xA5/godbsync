package util

import (
	"net"
	"net/url"
	"time"
)

type TimeoutConn struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	conn         net.Conn
}

func NewTimeoutConn(conn net.Conn) *TimeoutConn {
	return &TimeoutConn{
		conn: conn,
	}
}
func (c *TimeoutConn) SetReadTimeout(d time.Duration) error {
	return c.conn.SetReadDeadline(time.Now().Add(d))
}
func (c *TimeoutConn) Read(b []byte) (n int, err error) {
	n, err = c.conn.Read(b)
	if err != nil {
		c.SetReadTimeout(c.ReadTimeout)
	}
	return
}
func (c *TimeoutConn) SetWriteTimeout(d time.Duration) error {
	return c.conn.SetWriteDeadline(time.Now().Add(d))
}
func (c *TimeoutConn) Write(b []byte) (n int, err error) {
	c.SetWriteTimeout(c.WriteTimeout)
	n, err = c.conn.Write(b)
	return
}
func (c *TimeoutConn) Close() error {
	return c.conn.Close()
}

type TimeoutConfig struct {
	vs  url.Values
	err error
}

func ParseTimeoutConfig(s string) (*TimeoutConfig, error) {
	ts := new(TimeoutConfig)
	if s != "" {
		ts.vs, ts.err = url.ParseQuery(s)
	}
	return ts, ts.err
}
func (ts *TimeoutConfig) Get(key string, defaults time.Duration) (time.Duration, error) {
	if ts.err != nil {
		return defaults, ts.err
	}
	s := ts.vs.Get(key)
	if s == "" {
		return defaults, nil
	}
	d, err := time.ParseDuration(s)
	if d == 0 {
		d = defaults
	}
	return d, err
}
func (ts *TimeoutConfig) Set(key string, value time.Duration) {
	if ts.vs == nil {
		ts.vs = make(url.Values)
	}
	ts.vs.Set(key, value.String())
}

func (ts *TimeoutConfig) String() string {
	if ts.vs == nil {
		return ""
	}
	return ts.vs.Encode()
}
