package main

import (
	"encoding/json"
	"os"
)

type config struct {
	Log string

	Listen           string
	NotifyListen     string
	HttpListen       string
	NotifyServerAddr string
	NotifyServerName string
	Timeout          string
	PushTimeout      string

	ClientCA      string
	Cert, CertKey string

	DSNFile  string
	QueryLog string

	SyncTableName              string
	SyncColumns                string
	SyncClientBeforeFullUpdate string
	SyncClientInsert           string
	SyncFullUpdate             string
	SyncSingleUpdate           string
	UseLockTable               bool
}

var Config = &config{
	Log: "server.log",

	Listen:           ":9443",
	NotifyListen:     ":9444",
	HttpListen:       ":9445",
	NotifyServerName: "server",
	Timeout:          "read=60s&write=5s&heartbeat=25s",
	PushTimeout:      "read=60s&write=5s&heartbeat=25s",

	ClientCA: "cert/clientca.pem",
	Cert:     "cert/server.pem",
	CertKey:  "cert/server.key",

	DSNFile:  "db.dsn",
	QueryLog: "query.log",

	SyncClientBeforeFullUpdate: "",
	SyncClientInsert:           "INSERT INTO $_TABLE ($_COLUMNS) VALUES $_VALUES ON DUPLICATE KEY UPDATE $_ALL_VALUES",
	SyncFullUpdate:             "SELECT $_COLUMNS FROM $_TABLE",
	SyncSingleUpdate:           "SELECT $_COLUMNS FROM $_TABLE WHERE id=? LIMIT 1",
}

func (c *config) IsFileExist(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
func (c *config) Load(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	return dec.Decode(c)
}
func (c *config) Save(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	return enc.Encode(c)
}
