package main

import (
	"io"
	"log"
	"os"
	"time"
)

func main() {
	flog, err := os.OpenFile("dbsync.log", os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		log.Fatalf("FAILED write log: %v", err)
		return
	}
	defer flog.Close()
	log.SetOutput(io.MultiWriter(os.Stderr, flog))
	log.Println("info: app started")

	qlog, err := os.OpenFile("query.log", os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		log.Fatalf("FAILED write query log: %v", err)
		return
	}
	defer qlog.Close()
	DB.SetLogger(log.New(qlog, "", log.LstdFlags))

	dsn, err := readDSN("db.dsn")
	if err != nil {
		log.Fatalf("FAILED open db.dsn: %v", err)
		return
	}
	log.Printf("dsn: %s", dsn)

	if err = DB.Open(dsn); err != nil {
		log.Fatalf("FAILED connect to database: %v", err)
		return
	}

	client := new(Client)
	var prepareTLS = func(maxTry int) (err error) {
		for retry := 0; maxTry < 0 || retry < maxTry; retry++ {
			err = DB.CheckConn()
			if err != nil {
				log.Printf("ERROR DB.CheckConn: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			err = client.PrepareTLS()
			if err == nil {
				break
			}
			log.Printf("ERROR client.PrepareTLS (retry: %d): %v", retry, err)
			sleepByRetry(retry)
		}
		return
	}
	var connectServer = func(maxTry int) (err error) {
		for retry := 0; maxTry < 0 || retry < maxTry; retry++ {
			err = prepareTLS(maxTry)
			if err != nil {
				return
			}
			err = client.ConnectServer()
			if err == nil {
				return
			}
			log.Printf("ERROR client.ConnectServer (retry: %d): %v", retry, err)
			sleepByRetry(retry)
		}
		return
	}
	var connectNotify = func(maxTry int) (err error) {
		for retry := 0; maxTry < 0 || retry < maxTry; retry++ {
			if !client.SeverConnected() {
				return
			}
			err = client.ConnectNotify()
			log.Printf("ERROR client.ConnectNotify (retry: %d): %v", retry, err)
			sleepByRetry(retry)
		}
		return
	}

	err = connectServer(6)
	if err != nil {
		log.Printf("info: max retry reached, try recover last config")
		err = DB.CopyTable("sync_vars", "sync_vars__1")
		if err != nil {
			log.Printf("ERROR recover config from `sync_vars__1`: %v", err)
		} else {
			log.Printf("info: config recovered")
		}
	} else {
		err = DB.CopyTable("sync_vars__1", "sync_vars")
		if err != nil {
			log.Printf("ERROR save config to 'sync_vars__1': %v", err)
		} else {
			log.Printf("info: config saved")
		}
	}

	heartbeatStarted := false
	notifyConnected := false
	log.Printf("info: start monitoring")
	for {
		if !client.SeverConnected() {
			log.Printf("info: restart connect server")
			connectServer(-1)
		}
		if !heartbeatStarted && client.SeverConnected() {
			go func() {
				heartbeatStarted = true
				err := client.SetupHeartbeat()
				heartbeatStarted = false
				log.Printf("ERROR client.SetupHeartbeat: %v", err)
			}()
		}
		if !notifyConnected && client.SeverConnected() {
			log.Printf("info: connect notify server")
			go func() {
				defer func() {
					if p := recover(); p != nil {
						log.Printf("ERROR client.ConnectNotify painc: %v", p)
					}
				}()
				notifyConnected = true
				connectNotify(-1)
				notifyConnected = false
			}()
		}
		time.Sleep(1 * time.Second)
	}
}

func sleepByRetry(retry int) {
	var d time.Duration
	switch retry {
	case 0, 1, 2:
		d = 2 * time.Second
	case 3, 4, 5:
		d = 10 * time.Second
	case 6, 7, 8, 9, 10:
		d = 30 * time.Second
	default:
		d = 60 * time.Second
	}
	time.Sleep(d)
}
