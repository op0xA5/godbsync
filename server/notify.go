package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"strconv"
	"time"

	uuid "github.com/satori/go.uuid"
)

func handleNotifyConn(conn io.ReadWriteCloser) {
	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()

	var clientSendMessagef = func(format string, a ...interface{}) {
		var reply int32
		cma := new(ClientMessageArgs)
		cma.Message = fmt.Sprintf(format, a...)
		rpcClient.Call("client.Message", cma, &reply)
	}

	var clientUUID string
	if err := rpcClient.Call("client.GetValue", "client_uuid", &clientUUID); err != nil {
		log.Printf("ERROR rpc get clinet uuid: %v", err)
		clientSendMessagef("error get client uuid: %v", err)
		clientSendMessagef("server will close connection")
		return
	}

	if clientUUID == "" {
		u2, err := uuid.NewV4()
		if err != nil {
			log.Printf("ERROR generate uuid for client: %v", err)
			clientSendMessagef("error generate uuid: %v", err)
			clientSendMessagef("server will close connection")
			return
		}
		clientUUID = u2.String()
		log.Printf("info: new client UUID generated '%s'", clientUUID)
		// sql := "REPLACE INTO `sync_vars` () VALUES ()"
		clientSendMessagef("set client UUID '%s'", clientUUID)
	}

	var maxPacketSize = 4 * 1024
	var maxPacketSizeText string
	if err := rpcClient.Call("client.GetValue", "sql_max_allowed_packet", &maxPacketSizeText); err != nil {
		log.Printf("info: rpc client.GetValue sql_max_allowed_packet: %v", err)
		clientSendMessagef("error client.GetValue sql_max_allowed_packet: %v", err)
	}
	if maxPacketSizeText != "" {
		var err error
		maxPacketSize, err = strconv.Atoi(maxPacketSizeText)
		if err != nil {
			log.Printf("info: rpc client.GetValue sql_max_allowed_packet: %v (got '%s')", err, maxPacketSizeText)
			clientSendMessagef("error client.GetValue sql_max_allowed_packet: %v (got '%s')", err, maxPacketSizeText)
			maxPacketSize = 4 * 1024
		}
	}
	_ = maxPacketSize

	err := preSync(rpcClient, clientUUID)
	if err != nil {
		log.Printf("ERROR preSync[%s]: %v", clientUUID, err)
		clientSendMessagef("error preSync: %v", err)
		clientSendMessagef("server will close connection")
		return
	}

	var q *Queue
	defer func() {
		if q != nil {
			q.Close()
		}
	}()
	log.Printf("info: enter sync loop")
	for {
		q = DefaultQM.Get(clientUUID)
		if q == nil {
			log.Printf("info: start full sync[%s]", clientUUID)
			q, err = fullSync(clientUUID, DefaultQM, rpcClient, maxPacketSize)
			if err != nil {
				log.Printf("ERROR full sync[%s]: %v", clientUUID, err)
				clientSendMessagef("error full sync: %v", err)
				return
			}
		}

		for {
			res, err := q.RetrieveTimeout(time.Millisecond * 100)
			if err != nil {
				log.Printf("ERROR retrieve item[%s]: %v", clientUUID, err)
				clientSendMessagef("error retrieve item: %v", err)
				return
			}

			for len(res) > 0 {
				var sql string
				sql, res = SQL.ClientInsertSlice(res, maxPacketSize)
				if sql != "" {
					execArgs := DBQueryArgs{
						Command: sql,
					}
					execReply := DBExecReply{}
					err := rpcClient.Call("db.Exec", &execArgs, &execReply)
					if err != nil {
						log.Printf("ERROR rpc db.Exec[%s] 'INSERT INTO ...': %v", clientUUID, err)
						clientSendMessagef("error exec 'INSERT INTO': %v", err)
						clientSendMessagef("server will close connection")
						return
					}
					log.Printf("client db.Exec[%s] 'INSERT INTO ...', RowsAffected: %d",
						clientUUID, execReply.RowsAffected)
				}
			}
		}
	}

}

func preSync(rpcClient *rpc.Client, clientUUID string) error {
	return nil
}

func fullSync(clientUUID string, qm *QueueMap, rpcClient *rpc.Client, maxPacketSize int) (*Queue, error) {
	var err error
	var rows *sql.Rows
	var tx *sql.Tx

	defer func() {
		if tx != nil {
			tx.Exec(SQL.UnlockTable)
			tx.Commit()
		}
	}()

	if Config.UseLockTable {
		tx, err = DB.Conn().Begin()
		if err != nil {
			return nil, fmt.Errorf("begin transaction, %v", err)
		}
		_, err = tx.Exec(SQL.LockTable)
		if err != nil {
			return nil, fmt.Errorf("lock table, %v", err)
		}
		rows, err = tx.Query(SQL.SyncFullUpdate)
	} else {
		rows, err = DB.Conn().Query(SQL.SyncFullUpdate)
	}
	if err != nil {
		return nil, fmt.Errorf("query, %v", err)
	}
	defer rows.Close()
	d, err := MakeDbDump(rows, SQL.Columns)
	if err != nil {
		return nil, fmt.Errorf("dump, %v", err)
	}
	defer d.Close()

	if tx != nil {
		tx.Exec(SQL.UnlockTable)
		tx.Commit()
		tx = nil
	}

	q := NewQueue(clientUUID)
	qm.Add(q)

	if SQL.SyncClientBeforeFullUpdate != "" {
		execArgs := DBQueryArgs{
			Command: SQL.SyncClientBeforeFullUpdate,
		}
		execReply := DBExecReply{}
		err := rpcClient.Call("db.Exec", &execArgs, &execReply)
		if err != nil {
			err = fmt.Errorf("rpc db.Exec '%s': %v", execArgs.Command, err)
			return nil, err
		}
		log.Printf("client db.Exec[%s] '%s', RowsAffected: %d",
			clientUUID, execArgs.Command, execReply.RowsAffected)
	}

	for {
		sql, end := SQL.ClientInsertDump(d, maxPacketSize)
		if sql != "" {
			execArgs := DBQueryArgs{
				Command: sql,
			}
			execReply := DBExecReply{}
			err := rpcClient.Call("db.Exec", &execArgs, &execReply)
			if err != nil {
				err = fmt.Errorf("rpc db.Exec[%s] 'INSERT INTO ...': %v", clientUUID, err)
				return q, err
			}
			log.Printf("client db.Exec[%s] 'INSERT INTO ...', RowsAffected: %d",
				clientUUID, execReply.RowsAffected)
		}
		if end {
			break
		}
	}

	return q, nil
}
