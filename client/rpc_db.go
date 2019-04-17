package main

import (
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
)

type RpcDB int

type DBQueryArgs struct {
	Command string
	Params  []interface{}
	Stmt    int64
	Tx      int64
	Columns bool
}

type DBExecReply struct {
	LastInsertID int64
	RowsAffected int64
}
type DBQueryReply struct {
	Columns []string
	Cursor  int64
}
type DBQueryAllReply struct {
	Columns []string
	Results [][]string
}
type DBFetchArgs struct {
	Cursor int64
	Rows   int
}
type DBFetchReply struct {
	End     bool
	Results [][]string
}
type DBBeginReply struct {
	Tx int64
}
type DBPrepareReply struct {
	Stmt int64
}

type openfile struct {
	tx          *sql.Tx
	stmt        *sql.Stmt
	rows        *sql.Rows
	columnCount int
}

var openfileIDGenerator int64
var openfileCount int64
var openfileMap map[int64]*openfile
var openfileMu sync.RWMutex

var MaxOpenfileCount int64 = 32

func createOpenfile(of *openfile) (int64, error) {
	count := atomic.AddInt64(&openfileCount, 1)
	if count > MaxOpenfileCount {
		atomic.AddInt64(&openfileCount, -1)
		return 0, errors.New("too many open files")
	}

	id := atomic.AddInt64(&openfileIDGenerator, 1)
	openfileMu.Lock()
	openfileMap[id] = of
	openfileMu.Unlock()
	return id, nil
}
func retrieveOpenfile(id int64) (*openfile, error) {
	var of *openfile
	var ok bool

	openfileMu.RLock()
	of, ok = openfileMap[id]
	openfileMu.RUnlock()
	if ok {
		return of, nil
	}
	return nil, errors.New("file not found")
}
func closeOpenfile(id int64) {
	var countBefore, countAfter int
	openfileMu.Lock()
	countBefore = len(openfileMap)
	delete(openfileMap, id)
	countAfter = len(openfileMap)
	openfileMu.Unlock()
	if countAfter < countBefore {
		atomic.AddInt64(&openfileCount, -1)
	}
}

func (*RpcDB) Exec(args *DBQueryArgs, reply *DBExecReply) error {
	q, err := getQuerier(args)
	if err != nil {
		return err
	}

	qs := DB.BeforeQuery(args.Command, args.Params...)	
	result, err := q.Exec(qs.SQL, qs.Params...)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	reply.LastInsertID, _ = result.LastInsertId()
	reply.RowsAffected, _ = result.RowsAffected()
	return nil
}
func (*RpcDB) Query(args *DBQueryArgs, reply *DBQueryReply) error {
	q, err := getQuerier(args)
	if err != nil {
		return err
	}
	qs := DB.BeforeQuery(args.Command, args.Params...)
	rows, err := q.Query(qs.SQL, qs.Params...)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if args.Columns {
		reply.Columns = columns
	}
	id, err := createOpenfile(&openfile{rows: rows, columnCount: len(columns)})
	if err != nil {
		rows.Close()
		return err
	}
	reply.Cursor = id
	return nil
}
func (*RpcDB) QueryAll(args *DBQueryArgs, reply *DBQueryAllReply) error {
	q, err := getQuerier(args)
	if err != nil {
		return err
	}
	qs := DB.BeforeQuery(args.Command, args.Params...)
	rows, err := q.Query(qs.SQL, qs.Params...)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if args.Columns {
		reply.Columns = columns
	}

	results, _, err := fetch(rows, len(columns), -1)
	if err != nil {
		return err
	}
	reply.Results = results
	return nil
}
func (*RpcDB) QueryScalar(args *DBQueryArgs, reply *int64) error {
	q, err := getQuerier(args)
	if err != nil {
		return err
	}

	var rtn int64
	qs := DB.BeforeQuery(args.Command, args.Params...)
	err = q.QueryRow(qs.SQL, qs.Params...).Scan(&rtn)
	qs.EndQuery(err)
	if err != nil {
		return err
	}
	*reply = rtn
	return nil
}

type querier interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}
type stmtQuerier struct {
	stmt *sql.Stmt
}

func (q stmtQuerier) Exec(query string, args ...interface{}) (sql.Result, error) {
	return q.stmt.Exec(args...)
}
func (q stmtQuerier) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.stmt.Query(args...)
}
func (q stmtQuerier) QueryRow(query string, args ...interface{}) *sql.Row {
	return q.stmt.QueryRow(args...)
}

func getQuerier(args *DBQueryArgs) (querier, error) {
	if args.Stmt != 0 {
		of, errof := retrieveOpenfile(args.Stmt)
		if errof != nil {
			return nil, errof
		}
		if of.stmt == nil {
			return nil, errors.New("resource is not statement")
		}
		return stmtQuerier{of.stmt}, nil
	}
	if args.Tx != 0 {
		of, errof := retrieveOpenfile(args.Tx)
		if errof != nil {
			return nil, errof
		}
		if of.tx == nil {
			return nil, errors.New("resource is not transaction")
		}
		return of.tx, nil
	}
	return DB.Conn(), nil
}

func (*RpcDB) Fetch(args *DBFetchArgs, reply *DBFetchReply) (err error) {
	of, errof := retrieveOpenfile(args.Cursor)
	if errof != nil {
		return err
	}
	if of.rows == nil {
		return errors.New("resource is not cursor")
	}

	results, end, err := fetch(of.rows, of.columnCount, args.Rows)
	if err != nil || end {
		reply.End = true
		of.rows.Close()
		closeOpenfile(args.Cursor)
	}
	if err != nil {
		return err
	}
	reply.End = end
	reply.Results = results
	return
}
func fetch(rows *sql.Rows, columns int, count int) (results [][]string, end bool, err error) {
	if count <= 0 {
		count = 1 << 31
	}
	var sbuf []string
	makeColumn := func() (s []string) {
		if len(sbuf) < columns {
			batch := count
			if batch > 32 {
				batch = 32
			}
			sbuf = make([]string, batch*columns)
		}
		s = sbuf[:columns]
		sbuf = sbuf[columns:]
		return
	}
	scan := make([]interface{}, columns)
	for count > 0 {
		if !rows.Next() {
			end = true
			return
		}
		column := makeColumn()
		for i := range scan {
			scan[i] = &column[i]
		}
		if err = rows.Scan(scan...); err != nil {
			return
		}
		if results == nil {
			results = make([][]string, 0, count)
		}
		results = append(results, column)
		count--
	}
	return
}

func (*RpcDB) CloseCursor(id int64, reply *int) error {
	of, err := retrieveOpenfile(id)
	if err != nil {
		return err
	}
	if of.rows == nil {
		return errors.New("resource is not cursor")
	}
	*reply = 1
	closeOpenfile(id)
	return of.rows.Close()
}
func (*RpcDB) Begin(args int, reply *DBBeginReply) error {
	tx, err := DB.Conn().Begin()
	if err != nil {
		return err
	}

	id, err := createOpenfile(&openfile{tx: tx})
	if err != nil {
		if tx != nil {
			tx.Rollback()
		}
		return err
	}

	reply.Tx = id
	return nil
}
func (*RpcDB) Commit(id int64, reply *int) error {
	of, err := retrieveOpenfile(id)
	if err != nil {
		return err
	}
	if of.tx == nil {
		return errors.New("resource is not transaction")
	}
	*reply = 1
	closeOpenfile(id)
	return of.tx.Commit()
}
func (*RpcDB) Rollback(id int64, reply *int) error {
	of, err := retrieveOpenfile(id)
	if err != nil {
		return err
	}
	if of.tx == nil {
		return errors.New("resource is not transaction")
	}
	*reply = 1
	closeOpenfile(id)
	return of.tx.Rollback()
}
func (*RpcDB) Prepare(args DBQueryArgs, reply *DBPrepareReply) (err error) {
	var stmt *sql.Stmt
	if args.Tx != 0 {
		of, errof := retrieveOpenfile(args.Tx)
		if errof != nil {
			return errof
		}
		if of.tx == nil {
			return errors.New("resource is not transaction")
		}
		stmt, err = of.tx.Prepare(args.Command)
	} else {
		stmt, err = DB.Conn().Prepare(args.Command)
	}
	if err != nil {
		return err
	}

	id, err := createOpenfile(&openfile{stmt: stmt})
	if err != nil {
		if stmt != nil {
			stmt.Close()
		}
		return err
	}

	reply.Stmt = id
	return nil
}
func (*RpcDB) CloseStmt(id int64, reply *int) error {
	of, err := retrieveOpenfile(id)
	if err != nil {
		return err
	}
	if of.stmt == nil {
		return errors.New("resource is not statement")
	}
	*reply = 1
	closeOpenfile(id)
	return of.stmt.Close()
}
