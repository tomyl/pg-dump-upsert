package pgdump

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

type column struct {
	// Name of column
	Name string

	// Postgresql data type e.g. "text", "bigint" etc
	Type string

	// Whether column is nullable
	Nullable bool

	// Whether to include this column in INSERT statement
	insert bool

	// Whether to include this column in DO UPDATE SET list
	update bool

	// Scan destination for this column, see getScanDest().
	value interface{}
}

func (col *column) bind() {
	if col.value != nil {
		panic("alread bound column " + col.Name + " of type " + col.Type)
	}

	switch col.Type {
	case "bigint":
		if col.Nullable {
			var v sql.NullInt64
			col.value = &v
		} else {
			var v int64
			col.value = &v
		}
	case "boolean":
		if col.Nullable {
			var v sql.NullBool
			col.value = &v
		} else {
			var v bool
			col.value = &v
		}
	case "integer":
		if col.Nullable {
			var v sql.NullInt64
			col.value = &v
		} else {
			var v int
			col.value = &v
		}
	case "numeric":
		fallthrough
	case "text":
		if col.Nullable {
			var v sql.NullString
			col.value = &v
		} else {
			var v string
			col.value = &v
		}
	case "timestamp with time zone":
		if col.Nullable {
			var v pq.NullTime
			col.value = &v
		} else {
			var v time.Time
			col.value = &v
		}
	}

	if col.value == nil {
		if col.Nullable {
			panic("don't know how to bind nullable column " + col.Name + " of type " + col.Type)
		}
		panic("don't know how to bind column " + col.Name + " of type " + col.Type)
	}
}

func (col column) literal() string {
	if col.value == nil {
		panic("column " + col.Name + " of type " + col.Type + " is not bound")
	}

	switch col.Type {
	case "bigint":
		if col.Nullable {
			v := col.value.(*sql.NullInt64)
			if v.Valid {
				return strconv.FormatInt(v.Int64, 10)
			}
			return "NULL"
		}
		v := col.value.(*int64)
		return strconv.FormatInt(*v, 10)

	case "boolean":
		if col.Nullable {
			v := col.value.(*sql.NullBool)
			if v.Valid {
				if v.Bool {
					return "TRUE"
				}
				return "FALSE"
			}
			return "NULL"
		}
		v := col.value.(*int64)
		return strconv.FormatInt(*v, 10)

	case "integer":
		if col.Nullable {
			v := col.value.(*sql.NullInt64)
			if v.Valid {
				return strconv.FormatInt(v.Int64, 10)
			}
			return "NULL"
		}
		v := col.value.(*int)
		return strconv.Itoa(*v)

	case "numeric":
		if col.Nullable {
			v := col.value.(*sql.NullString)
			if v.Valid {
				return v.String
			}
			return "NULL"
		}
		v := col.value.(*string)
		return *v

	case "text":
		if col.Nullable {
			v := col.value.(*sql.NullString)
			if v.Valid {
				return quoteString(v.String)
			}
			return "NULL"
		}
		v := col.value.(*string)
		return quoteString(*v)

	case "timestamp with time zone":
		if col.Nullable {
			v := col.value.(*pq.NullTime)
			if v.Valid {
				ts := v.Time.Format("2006-01-02 15:04:05.000000-07")
				return "'" + ts + "'"
			}
			return "NULL"
		}
		v := col.value.(*time.Time)
		ts := v.Format("2006-01-02 15:04:05.000000-07")
		return "'" + ts + "'"
	}

	if col.Nullable {
		panic("don't know how to quote nullable column " + col.Name + " of type " + col.Type)
	}

	panic("don't know how to quote column " + col.Name + " of type " + col.Type)
}

// quoteString returns an SQL string literal.
func quoteString(s string) string {
	return "'" + strings.Replace(s, "'", "''", -1) + "'"
}

// getColumns fetches column list for table from database.
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
