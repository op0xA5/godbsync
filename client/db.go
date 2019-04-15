package main

import (
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type db struct {
	logger *log.Logger
	conn   *sql.DB
}

var DB = new(db)

func (db *db) SetLogger(logger *log.Logger) {
	db.logger = logger
}
func (db *db) Open(dsn string) error {
	var err error
	db.conn, err = sql.Open("mysql", dsn)
	return err
}
func (db *db) Conn() *sql.DB {
	return db.conn
}
func (db *db) CheckConn() error {
	_sql := "SELECT 'ping'"
	var out string
	return db.conn.QueryRow(_sql).Scan(&out)
}
func (db *db) CopyTable(dst, src string) error {
	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			tx.Rollback()
		}
	}()

	// Step 0. Get source table struct
	qs := db.BeforeQuery("SHOW CREATE TABLE `" + src + "`")
	var table, createTable string
	err = tx.QueryRow(qs.SQL).Scan(&table, &createTable)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	// replace table name with dst table
	pos := strings.IndexByte(createTable, '(')
	if pos < 12 { /* length of "CREATE TABLE" */
		return errors.New("bad create table syntax")
	}
	createTable = "CREATE TABLE `" + dst + "` " + createTable[pos:]

	// Step 1. Drop target table
	qs = db.BeforeQuery("DROP TABLE IF EXISTS `" + dst + "`")
	_, err = tx.Exec(qs.SQL)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	// Step 2. Create target table
	qs = db.BeforeQuery(createTable)
	_, err = tx.Exec(qs.SQL)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	// Step 3. Copy data
	qs = db.BeforeQuery("INSERT INTO `" + dst + "` SELECT * FROM `" + src + "`")
	_, err = tx.Exec(qs.SQL)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	rollback = false
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
func (db *db) GetValue(name string) (string, error) {
	qs := db.BeforeQuery("SELECT value FROM sync_vars WHERE name=?", name)
	row := DB.conn.QueryRow(qs.SQL, qs.Params...)
	var value string
	err := row.Scan(&value)
	qs.EndQuery(err)
	if err == sql.ErrNoRows {
		err = nil
	}
	return value, err
}
func (db *db) RequireValue(name string) (string, error) {
	val, err := db.GetValue(name)
	if val == "" {
		err = errors.New("value not set")
	}
	return val, err
}
func (db *db) SetValue(name string, value string) error {
	qs := db.BeforeQuery("INSERT INTO sync_vars(name, value) VALUES (?,?) ON DUPLICATE KEY UPDATE value=VALUES(value)", name, value)
	_, err := DB.conn.Exec(qs.SQL, qs.Params...)
	qs.EndQuery(err)
	return err
}

type KeyValuePair struct {
	Name  string
	Value string
}

func (db *db) GetValues(name string) ([]KeyValuePair, error) {
	qs := db.BeforeQuery("SELECT name,value FROM sync_vars WHERE name LIKE ?", name)
	res, err := DB.conn.Query(qs.SQL, qs.Params...)
	qs.EndQuery(err)
	if err != nil {
		return nil, err
	}
	var values []KeyValuePair
	for res.Next() {
		var name, val string
		err = res.Scan(&name, &val)
		if err != nil {
			return values, err
		}
		values = append(values, KeyValuePair{name, val})
	}
	return values, res.Err()
}

func (db *db) BeforeQuery(sql string, params ...interface{}) QueryStat {
	return QueryStat{
		logger: db.logger,
		SQL:    sql,
		Params: params,
		stime:  time.Now(),
	}
}

type QueryStat struct {
	SQL    string
	Params []interface{}

	logger *log.Logger
	stime  time.Time
}

func (qs QueryStat) EndQuery(err error) {
	d := time.Since(qs.stime)
	if err == nil {
		if len(qs.Params) == 0 {
			qs.logger.Printf("%q %.2fms OK", qs.SQL, d.Seconds()*1000)
		} else {
			qs.logger.Printf("%q %q %.2fms OK", qs.SQL, qs.Params, d.Seconds()*1000)
		}

	} else if err == sql.ErrNoRows {
		if len(qs.Params) == 0 {
			qs.logger.Printf("%q %.2fms NO_ROWS", qs.SQL, d.Seconds()*1000)
		} else {
			qs.logger.Printf("%q %q %.2fms NO_ROWS", qs.SQL, qs.Params, d.Seconds()*1000)
		}
	} else {

		if len(qs.Params) == 0 {
			qs.logger.Printf("%q %.2fms ERR %q", qs.SQL, d.Seconds()*1000, err.Error())
		} else {
			qs.logger.Printf("%q %q %.2fms ERR %q", qs.SQL, qs.Params, d.Seconds()*1000, err.Error())
		}
	}
}

const (
	ValueClientID      = "client_uuid"
	ValueServerAddr    = "server"
	ValueServerName    = "server_name"
	ValueServerCA      = "server_ca%"
	ValueCert          = "cert"
	ValueCertKey       = "cert_key"
	ValueTimeoutConfig = "timeout_config"
)
