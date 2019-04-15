package main

import (
	"database/sql"
	"log"
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
