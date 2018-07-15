package pgdump

import (
	"bytes"
	"database/sql"
)

func getQueryStatement(db *sql.DB, table string, cols []column) string {
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

	return buf.String()
}

func getInsertStatement(table string, cols []column, opts *Options) string {
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

	if opts.ConflictColumn != "" {
		buf.WriteString(" ON CONFLICT (" + opts.ConflictColumn + ") DO UPDATE SET ")
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
	} else if opts.NoConflict {
		buf.WriteString(" ON CONFLICT DO NOTHING")
	}

	buf.WriteString(";\n")

	return buf.String()
}
