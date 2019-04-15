package main

import (
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	uuid "github.com/satori/go.uuid"
)

type SyncColumn struct {
	Name     string
	SQLName  string
	IsString bool
}

type SyncColumns []*SyncColumn

func ParseSyncColumn(s string) (*SyncColumn, error) {
	if s == "" {
		return nil, errors.New("empty column name")
	}
	sc := new(SyncColumn)
	sc.Name = s
	if strings.HasPrefix(s, "$") {
		sc.Name = strings.TrimPrefix(s, "$")
		sc.IsString = true
	}
	sc.SQLName = "`" + sc.Name + "`"
	return sc, nil
}

func ParseSyncColumns(s string) (SyncColumns, error) {
	p := strings.Split(s, ",")
	scs := make([]*SyncColumn, len(p))
	var err error
	for i, v := range p {
		v = strings.TrimSpace(v)
		scs[i], err = ParseSyncColumn(v)
		if err != nil {
			return scs, fmt.Errorf("parse '%s': %v", v, err)
		}
	}
	return scs, nil
}

func (scs SyncColumns) String() string {
	s := make([]string, len(scs))
	for i := range scs {
		s[i] = scs[i].SQLName
	}
	return strings.Join(s, ",")
}

func (scs SyncColumns) ScanRow(row *sql.Row) ([]string, error) {
	res := make([]string, len(scs))
	dest := make([]interface{}, len(scs))
	for i := range res {
		dest[i] = &res[i]
	}
	err := row.Scan(dest...)
	return res, err
}
func (scs SyncColumns) Scan(res []string, rows *sql.Rows, dest []interface{}) error {
	if len(res) != len(scs) {
		return errors.New("res length not match columns count")
	}
	if dest == nil {
		dest = make([]interface{}, len(scs))
	}
	if len(dest) != len(scs) {
		return errors.New("dest length not match columns count")
	}
	for i := range res {
		dest[i] = &res[i]
	}
	return rows.Scan(dest...)
}

func (scs SyncColumns) AppendValues(sb *strings.Builder, v []string) error {
	if len(scs) != len(v) {
		return errors.New("SyncColumns.AppendValues: len(values) not match columns")
	}
	sb.WriteByte('(')
	for i, s := range v {
		if i > 0 {
			sb.WriteByte(',')
		}
		if scs[i].IsString {
			sb.Grow(len(s) * 2)
			sb.WriteByte('\'')
			last := 0
			for j, r := range s {
				if r == '\'' {
					sb.WriteString(s[last:j])
					sb.WriteByte('\'')
					sb.WriteByte('\'')
					last = j + 1
				}
			}
			sb.WriteString(s[last:])
			sb.WriteByte('\'')
		} else {
			sb.WriteString(s)
		}
	}
	sb.WriteByte(')')
	return nil
}

func (scs SyncColumns) AppendSetAllValues(sb *strings.Builder) {
	for i, sc := range scs {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(sc.SQLName)
		sb.WriteString("=VALUES(")
		sb.WriteString(sc.Name)
		sb.WriteByte(')')
	}
}

type SQLTemplet struct {
	table     string
	Columns   SyncColumns
	columnStr string

	SyncClientBeforeFullUpdate string
	syncClientInsert           string
	SyncFullUpdate             string
	SyncSingleUpdate           string

	LockTable   string
	UnlockTable string

	insertHead string
	insertFoot string
}

var SQL = new(SQLTemplet)

func (st *SQLTemplet) Init(config *config) error {
	var err error
	st.Columns, err = ParseSyncColumns(config.SyncColumns)
	if err != nil {
		return err
	}

	st.table = "`" + config.SyncTableName + "`"
	st.columnStr = st.Columns.String()

	st.SyncClientBeforeFullUpdate = st.templet(config.SyncClientBeforeFullUpdate)
	st.syncClientInsert = st.templet(config.SyncClientInsert)
	st.SyncFullUpdate = st.templet(config.SyncFullUpdate)
	st.SyncSingleUpdate = st.templet(config.SyncSingleUpdate)

	st.LockTable = st.templet("LOCK TABLES $_TABLE READ")
	st.UnlockTable = st.templet("UNLOCK TABLES")

	st.insertHead = st.templet("INSERT INTO $_TABLE($_COLUMNS) VALUES ")
	var sb strings.Builder
	st.Columns.AppendSetAllValues(&sb)
	st.insertFoot = "ON DUPLICATE KEY UPDATE " + sb.String()
	return nil
}

func (st *SQLTemplet) templet(s string) string {
	s = strings.Replace(s, "$_TABLE", st.table, -1)
	s = strings.Replace(s, "$_COLUMNS", st.columnStr, -1)
	return s
}

func (st *SQLTemplet) ClientInsertDump(res *DbDump, maxPacketSize int) (string, bool) {
	var sb strings.Builder
	var end bool

	comma := false
	for {
		if !res.Next() {
			end = true
			break
		}
		if sb.Len() == 0 {
			sb.WriteString(st.insertHead)
		}

		sbb := sb
		if comma {
			sb.WriteString(",")
		}
		st.Columns.AppendValues(&sb, res.Value())
		comma = true
		if sb.Len()+len(st.insertFoot) > maxPacketSize {
			sb = sbb
			break
		}
	}

	if sb.Len() > 0 {
		sb.WriteString(st.insertFoot)
	}

	return sb.String(), end
}

func (st *SQLTemplet) ClientInsertSlice(res [][]string, maxPacketSize int) (string, [][]string) {
	var sb strings.Builder
	var i int

	comma := false
	for i < len(res) {
		if sb.Len() == 0 {
			sb.WriteString(st.insertHead)
		}

		sbb := sb
		if comma {
			sb.WriteString(",")
		}
		st.Columns.AppendValues(&sb, res[i])
		comma = true
		if sb.Len()+len(st.insertFoot) > maxPacketSize {
			sb = sbb
			break
		}

		i++
	}

	if sb.Len() > 0 {
		sb.WriteString(st.insertFoot)
	}

	return sb.String(), res[i:]
}

type DbDump struct {
	f   *os.File
	dec *gob.Decoder
	val []string
	err error
}

func MakeDbDump(rows *sql.Rows, columns SyncColumns) (*DbDump, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	f, err := os.Create(fmt.Sprintf("%s.dump", id.String()))
	if err != nil {
		return nil, err
	}
	enc := gob.NewEncoder(f)

	res := make([]string, len(columns))
	dest := make([]interface{}, len(columns))
	for rows.Next() {
		err := columns.Scan(res, rows, dest)
		if err != nil {
			f.Close()
			return nil, err
		}
		enc.Encode(res)
	}
	if err := rows.Err(); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		f.Close()
		return nil, err
	}
	return &DbDump{f: f}, nil
}

func (d *DbDump) Next() bool {
	if d.dec == nil {
		if _, err := d.f.Seek(0, os.SEEK_SET); err != nil {
			d.err = err
		}
		d.dec = gob.NewDecoder(d.f)
	}
	if d.err != nil {
		return false
	}
	d.err = d.dec.Decode(&d.val)
	return d.err == nil
}
func (d *DbDump) Value() []string {
	return d.val
}
func (d *DbDump) Err() error {
	if d.err == io.EOF {
		return nil
	}
	return d.err
}
func (d *DbDump) Close() error {
	fname := d.f.Name()
	d.f.Close()
	return os.Remove(fname)
}
