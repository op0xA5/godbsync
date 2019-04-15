package main

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

func (*RpcDB) Exec(args *DBQueryArgs, reply *DBExecReply) error {
	panic("not implements")
}
func (*RpcDB) Query(args *DBQueryArgs, reply *DBQueryReply) error {
	panic("not implements")
}
func (*RpcDB) QueryAll(args *DBQueryArgs, reply *DBQueryAllReply) error {
	panic("not implements")
}
func (*RpcDB) QueryScalar(args *DBQueryArgs, reply *int64) error {
	panic("not implements")
}
func (*RpcDB) Fetch(args *DBFetchArgs, reply *DBFetchReply) (err error) {
	panic("not implements")
}
func (*RpcDB) Commit(id int64, reply *int) error {
	panic("not implements")
}
func (*RpcDB) Rollback(id int64, reply *int) error {
	panic("not implements")
}
func (*RpcDB) Prepare(args DBQueryArgs, reply *DBPrepareReply) (err error) {
	panic("not implements")
}
func (*RpcDB) CloseStmt(id int64, reply *int) error {
	panic("not implements")
}
