package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type HandleNotify int

func (*HandleNotify) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	ids := r.PostForm["id"]
	if ids == nil || len(ids) == 0 {
		http.Error(w, "no item", http.StatusOK)
		return
	}

	for _, id := range ids {
		row := DB.Conn().QueryRow(SQL.SyncSingleUpdate, id)
		res, err := SQL.Columns.ScanRow(row)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error Query Database: %v", err), http.StatusInternalServerError)
			return
		}
		DefaultQM.Append(res)
	}
	http.Error(w, fmt.Sprintf("OK, %d item processed", len(ids)), http.StatusOK)
	return
}

type HandleStat int

func (*HandleStat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.Encode(Stat)
}

func init() {
	http.DefaultServeMux.Handle("/notify", new(HandleNotify))
	http.DefaultServeMux.Handle("/stat", new(HandleStat))
}
