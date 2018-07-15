// Package pgdump provides functions to dump a Postgres table as INSERT statements.
package pgdump

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
)

// Options type controls how INSERT statements are constructed.
type Options struct {
	// Custom SELECT query. Defaults to fetch all rows. Can also just specify
	// the WHERE clause, it wil be appended to the default query.
	Query string

	// Which columns to include in INSERT statement. Defaults to all columns if empty.
	InsertColumns []string

	// Append a ON CONFLICT clause for this column. All other columns will end
	// up in a DO UPDATE SET list.
	ConflictColumn string

	// Append ON CONFLICT DO NOTHING clause.
	NoConflict bool

	// Log query statement to stderr.
	Verbose bool
}

// Dump outputs INSERT statements for all rows in specified table.
func Dump(writer io.Writer, db *sql.DB, table string, opts *Options) error {
	if opts == nil {
		opts = &Options{}
	}

	// Ask database for column list for this table
	cols, err := getColumns(db, table, opts)

	if err != nil {
		return err
	}

	// Verify insert/conflict columns exist
	if err := validateColumns(cols, opts); err != nil {
		return err
	}

	// Query rows to dump
	st := opts.Query

	if st == "" {
		st = getQueryStatement(db, table, cols)
	} else if strings.HasPrefix(strings.ToUpper(st), "WHERE") {
		where := st
		st = getQueryStatement(db, table, cols) + " " + where
	}

	if opts.Verbose {
		log.Println(st)
	}

	rows, err := db.Query(st)

	if err != nil {
		return err
	}

	defer rows.Close()

	dest := getScanDest(cols)
	count := 0

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		// Output INSERT statements
		st := getInsertStatement(table, cols, opts)
		writer.Write([]byte(st))
		count++
	}

	if opts.Verbose {
		log.Printf("Fetched %d rows", count)
	}

	return rows.Err()
}

func validateColumns(cols []column, opts *Options) error {
	if len(opts.InsertColumns) == 0 {
		// Dump all columns
		for i := range cols {
			cols[i].insert = true
		}
	} else {
		// Dump specified columns
		for _, colname := range opts.InsertColumns {
			found := false
			for i := range cols {
				if cols[i].Name == colname {
					cols[i].insert = true
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unknown insert column %s", colname)
			}
		}
	}

	// Verify conflict column exists
	foundconflictcol := false

	if opts.ConflictColumn != "" {
		for i := range cols {
			if cols[i].Name == opts.ConflictColumn {
				foundconflictcol = true
			} else if cols[i].insert {
				// Add column to DO UPDATE SET list
				cols[i].update = true
			}
		}
		if !foundconflictcol {
			return fmt.Errorf("no column %s", opts.ConflictColumn)
		}
	}

	return nil
}

func getScanDest(cols []column) []interface{} {
	var values []interface{}

	for _, col := range cols {
		if col.insert {
			values = append(values, col.value)
		}
	}

	return values
}
