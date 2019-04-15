package main

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"strings"
	"util"
)

type RpcClient int

type ClientConnectArgs struct {
	ClientID     string
	ClientVerson int32
}
type ClientConnectReply struct {
	ServerVersion int32
	Message       string
}

type ClientMessageArgs struct {
	ClientID string
	Message  string
}

type ClientRestartArgs struct {
	Magic   int64
	Message string
}

func (*RpcClient) Connect(args *ClientConnectArgs, reply *ClientConnectReply) error {
	return errors.New("not supported")
}
func (*RpcClient) GetValue(key string, value *string) (err error) {
	if strings.HasPrefix(key, "sql_") && !strings.Contains(key, "%") && !strings.Contains(key, "'") {
		qs := DB.BeforeQuery("SHOW GLOBAL VARIABLES LIKE '" + strings.TrimPrefix(key, "sql_") + "'")
		row := DB.conn.QueryRow(qs.SQL)
		var _dummy string
		err := row.Scan(&_dummy, value)
		qs.EndQuery(err)
		if err == sql.ErrNoRows {
			err = nil
		}
		return err
	}

	switch key {
	case "client_uuid":
		*value, err = DB.GetValue(ValueClientID)
	case "timeout_config":
		var v string
		v, err = DB.GetValue(ValueTimeoutConfig)
		if err != nil {
			return err
		}
		tc, _ := util.ParseTimeoutConfig(v)
		read, _ := tc.Get("read", DefaultReadTimeout)
		write, _ := tc.Get("write", DefaultWriteTimeout)
		heartbeat, _ := tc.Get("heartbeat", DefaultHeartbeatTimeout)
		tc.Set("read", read)
		tc.Set("write", write)
		tc.Set("heartbeat", heartbeat)
		*value = tc.String()
	default:
		err = errors.New("unknown key")
	}
	return
}
func (*RpcClient) Message(args *ClientMessageArgs, reply *int32) error {
	log.Print("server message: ", args.Message)
	return nil
}
func (*RpcClient) Ping(args int64, reply *int64) error {
	*reply = args
	return nil
}

func (*RpcClient) Restart(args *ClientRestartArgs, reply *int64) error {
	if args.Magic != 0x1122334455667788 {
		return errors.New("code mismatch")
	}
	if args.Message == "" {
		return errors.New("message required")
	}
	log.Printf("server claim restart: %s", args.Message)
	// TODO: some clean up
	os.Exit(-127)
	return nil
}
