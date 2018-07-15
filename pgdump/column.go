package pgdump

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

type column struct {
	Name     string
	Type     string
	Nullable bool

	Insert bool
	Update bool
	Value  interface{}
}

func (col *column) bind() {
	if col.Value != nil {
		panic("alread bound column " + col.Name + " of type " + col.Type)
	}

	switch col.Type {
	case "bigint":
		if col.Nullable {
			var v sql.NullInt64
			col.Value = &v
		} else {
			var v int64
			col.Value = &v
		}
	case "boolean":
		if col.Nullable {
			var v sql.NullBool
			col.Value = &v
		} else {
			var v bool
			col.Value = &v
		}
	case "integer":
		if col.Nullable {
			var v sql.NullInt64
			col.Value = &v
		} else {
			var v int
			col.Value = &v
		}
	case "numeric":
		fallthrough
	case "text":
		if col.Nullable {
			var v sql.NullString
			col.Value = &v
		} else {
			var v string
			col.Value = &v
		}
	case "timestamp with time zone":
		if col.Nullable {
			var v pq.NullTime
			col.Value = &v
		} else {
			var v time.Time
			col.Value = &v
		}
	}

	if col.Value == nil {
		if col.Nullable {
			panic("don't know how to bind nullable column " + col.Name + " of type " + col.Type)
		}
		panic("don't know how to bind column " + col.Name + " of type " + col.Type)
	}
}

func (col column) Literal() string {
	switch col.Type {
	case "bigint":
		if col.Nullable {
			v := col.Value.(*sql.NullInt64)
			if v.Valid {
				return strconv.FormatInt(v.Int64, 10)
			}
			return "NULL"
		} else {
			v := col.Value.(*int64)
			return strconv.FormatInt(*v, 10)
		}
	case "boolean":
		if col.Nullable {
			v := col.Value.(*sql.NullBool)
			if v.Valid {
				if v.Bool {
					return "TRUE"
				} else {
					return "FALSE"
				}
			}
			return "NULL"
		} else {
			v := col.Value.(*int64)
			return strconv.FormatInt(*v, 10)
		}
	case "integer":
		if col.Nullable {
			v := col.Value.(*sql.NullInt64)
			if v.Valid {
				return strconv.FormatInt(v.Int64, 10)
			}
			return "NULL"
		} else {
			v := col.Value.(*int)
			return strconv.Itoa(*v)
		}
	case "numeric":
		if col.Nullable {
			v := col.Value.(*sql.NullString)
			if v.Valid {
				return v.String
			}
			return "NULL"
		} else {
			v := col.Value.(*string)
			return *v
		}
	case "text":
		if col.Nullable {
			v := col.Value.(*sql.NullString)
			if v.Valid {
				return quoteString(v.String)
			}
			return "NULL"
		} else {
			v := col.Value.(*string)
			return quoteString(*v)
		}
	case "timestamp with time zone":
		if col.Nullable {
			v := col.Value.(*pq.NullTime)
			if v.Valid {
				ts := v.Time.Format("2006-01-02 15:04:05.000000-07")
				return "'" + ts + "'"
			}
			return "NULL"
		} else {
			v := col.Value.(*time.Time)
			ts := v.Format("2006-01-02 15:04:05.000000-07")
			return "'" + ts + "'"
		}
	}

	if col.Nullable {
		panic("don't know how to quote nullable column " + col.Name + " of type " + col.Type)
	}

	panic("don't know how to quote column " + col.Name + " of type " + col.Type)
}

func quoteString(s string) string {
	return "'" + strings.Replace(s, "'", "''", -1) + "'"
}

func getColumns(db *sql.DB, table string) ([]column, error) {
	rows, err := db.Query("select column_name, data_type, is_nullable from information_schema.columns where table_name=$1", table)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	cols := make([]column, 0)

	for rows.Next() {
		var col column
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, err
		}
		if nullable == "YES" {
			col.Nullable = true
		}
		// FIXME: don't bind/add all columns if -insert is used
		col.bind()
		cols = append(cols, col)
	}

	return cols, rows.Err()
}
