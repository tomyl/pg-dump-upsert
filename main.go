package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
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

func (col *column) Bind() {
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
		col.Bind()
		cols = append(cols, col)
	}

	return cols, rows.Err()
}

func queryRows(db *sql.DB, table string, cols []column) (*sql.Rows, error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT ")

	count := 0

	for _, col := range cols {
		if col.Insert {
			if count > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.Name)
			count++
		}
	}

	buf.WriteString(" FROM " + table)
	st := buf.String()
	log.Print(st)

	rows, err := db.Query(st)

	if err != nil {
		return nil, err
	}

	return rows, err
}

func getInsertString(table string, cols []column, conflict string, noconflict bool) string {
	var buf bytes.Buffer

	buf.WriteString("INSERT INTO " + table + " (")
	count := 0

	for _, col := range cols {
		if col.Insert {
			if count > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.Name)
			count++
		}
	}

	buf.WriteString(") VALUES (")
	count = 0

	for _, col := range cols {
		if col.Insert {
			if count > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.Literal())
			count++
		}
	}

	buf.WriteString(")")

	if conflict != "" {
		buf.WriteString(" ON CONFLICT (" + conflict + ") DO UPDATE SET ")
		count = 0
		for _, col := range cols {
			if col.Update {
				if count > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.Name + "=EXCLUDED." + col.Name)
				count++
			}
		}
	} else if noconflict {
		buf.WriteString(" ON CONFLICT DO NOTHING")
	}

	buf.WriteString(";")

	return buf.String()
}

func dump(dsn, table string, insertcols []string, conflict string, noconflict bool) error {
	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return err
	}

	cols, err := getColumns(db, table)

	if err != nil {
		return err
	}

	if len(insertcols) == 0 {
		for i := range cols {
			cols[i].Insert = true
		}
	} else {
		for _, colname := range insertcols {
			colname = strings.TrimSpace(colname)
			found := false
			for i := range cols {
				if cols[i].Name == colname {
					cols[i].Insert = true
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unknown column %s", colname)
			}
		}
	}

	foundconflictcol := false

	if conflict != "" {
		for i := range cols {
			if cols[i].Name == conflict {
				foundconflictcol = true
			} else if cols[i].Insert {
				cols[i].Update = true
			}
		}
		if !foundconflictcol {
			return fmt.Errorf("no column %s", conflict)
		}
	}

	rows, err := queryRows(db, table, cols)

	if err != nil {
		return err
	}

	defer rows.Close()

	var values []interface{}

	for _, col := range cols {
		if col.Insert {
			values = append(values, col.Value)
		}
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return err
		}
		fmt.Println(getInsertString(table, cols, conflict, noconflict))
	}

	return rows.Err()
}

func main() {
	dsn := flag.String("dsn", "", "Connection string. Example: postgres://user:password@localhost:5432/db?sslmode=disable")
	table := flag.String("table", "", "Table to dump.")
	insert := flag.String("insert", "", "What columns to include in INSERT statement. Defaults to all columns")
	conflict := flag.String("conflict-column", "", "Append an ON CONFLICT clause for this column. All other columns will be included in a DO UPDATE SET list.")
	noconflict := flag.Bool("noconflict", false, "Append ON CONFLICT DO NOTHING.")
	tx := flag.Bool("tx", false, "Wrap INSERT statements in transaction.")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("-dsn not supplied")
	}

	if *table == "" {
		log.Fatal("-table not supplied")
	}

	if *noconflict && *conflict != "" {
		log.Fatal("cannot combine -noconflict and -conflict")
	}

	insertcols := strings.Split(*insert, ",")

	if *tx {
		fmt.Printf("BEGIN;\n")
	}

	if err := dump(*dsn, *table, insertcols, *conflict, *noconflict); err != nil {
		log.Fatal(err)
	}

	if *tx {
		fmt.Printf("COMMIT;\n")
	}
}
