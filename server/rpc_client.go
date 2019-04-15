package main

import (
	"errors"
	"log"
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
	log.Printf("client Connect: clinetID='%s', clientVersion='%d'", args.ClientID, args.ClientVerson)
	reply.ServerVersion = ServerVersion
	return nil
}
func (*RpcClient) GetValue(key string, value *string) (err error) {
	switch key {
	case "notify_server_addr":
		if Config.NotifyServerName != "" {
			*value = Config.NotifyServerAddr + "<" + Config.NotifyServerName + ">"
		} else {
			*value = Config.NotifyServerAddr
		}
	case "timeout_config":
		*value = Config.PushTimeout
	default:
		err = errors.New("unknown key")
	}
	return
}
func (*RpcClient) Message(args *ClientMessageArgs, reply *int32) error {
	log.Printf("client message: [%s] %s", args.ClientID, args.Message)
	return nil
}
func (*RpcClient) Ping(args int64, reply *int64) error {
	*reply = args
	return nil
}

func (*RpcClient) Restart(args *ClientRestartArgs, reply *int64) error {
	return errors.New("no way")
}
