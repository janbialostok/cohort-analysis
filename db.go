package main

import (
	"database/sql"
	"os"

	"github.com/huandu/go-sqlbuilder"
	_ "github.com/mattn/go-sqlite3"
)

type SQL interface {
	Close() error
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
}

// ConnectDB creates a sqlite db instance and optionally drops any existing tables
func ConnectDB(drop bool, dbname string) (SQL, error) {
	if drop {
		os.Remove(dbname)
	}
	db, err := sql.Open("sqlite3", dbname)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CreateTable(db SQL, table string, columns map[string]string) error {
	builder := sqlbuilder.NewCreateTableBuilder().
		CreateTable(table).
		IfNotExists()

	for col, typedef := range columns {
		builder.Define(col, typedef)
	}

	statement, args := builder.Build()
	if _, err := db.Exec(statement, args...); err != nil {
		return err
	}
	return nil
}

func Insert(db SQL, table string, values []interface{}) error {
	builder := sqlbuilder.NewInsertBuilder().
		InsertInto(table).
		Values(values...)

	statement, args := builder.Build()
	if _, err := db.Exec(statement, args...); err != nil {
		return err
	}
	return nil
}

type QueryOptions struct {
	OrderBy string
	Asc     bool
	Where   string
	Limit   int
	Offset  int
}

func Query(db SQL, table string, sel []string, options QueryOptions) (*sql.Rows, error) {
	builder := sqlbuilder.NewSelectBuilder().
		From(table).
		Select(sel...)

	if options.Where != "" {
		builder.Where(options.Where)
	}

	if options.OrderBy != "" {
		builder.OrderBy(options.OrderBy)
		if options.Asc {
			builder.Asc()
		} else {
			builder.Desc()
		}
	}

	if options.Limit != 0 {
		builder.Limit(options.Limit)
	}

	if options.Offset != 0 {
		builder.Offset(options.Offset)
	}

	statement, args := builder.Build()
	result, err := db.Query(statement, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}
