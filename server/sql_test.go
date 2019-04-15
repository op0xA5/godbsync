package main

import (
	"database/sql"
	"testing"
)

func TestDbDump(t *testing.T) {

	cs, err := ParseSyncColumns("id,$bus_plate,$valid_start,$valid_end,valid_count,valid_status,$valid_entity,$valid_device_id,$title,$last_modify")
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("mysql", "huaruitest:18660186541@tcp(mysqladminok.chenenke.net:873)/huaruierp121")
	if err != nil {
		t.Fatal(err)
	}
	sql := "SELECT id,bus_plate,valid_start,valid_end,valid_count,valid_status,valid_entity,valid_device_id,title,last_modify FROM bus_authorized"

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatal(err)
	}

	d, err := MakeDbDump(rows, cs)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for d.Next() {
		t.Log(d.Value())
	}
	if err = d.Err(); err != nil {
		t.Fatal(err)
	}
}
