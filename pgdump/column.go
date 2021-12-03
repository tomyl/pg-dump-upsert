package pgdump

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type column struct {
	// Name of column
	Name string

	// Postgresql data type e.g. "text", "bigint" etc
	Type string

	// Whether column is nullable
	Nullable bool

	// Whether column is an array
	Array bool

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
	case "smallint", "integer", "bigint", "smallserial", "serial", "bigserial":
		if col.Array {
			col.value = new(pq.Int64Array)
		} else if col.Nullable {
			col.value = new(sql.NullInt64)
		} else {
			col.value = new(int64)
		}
	case "real", "double precision":
		if col.Array {
			col.value = new(pq.Float64Array)
		} else if col.Nullable {
			col.value = new(sql.NullFloat64)
		} else {
			col.value = new(float64)
		}
	case "decimal", "numeric", "money", "character varying", "varchar", "character", "char", "text", "binary", "json", "jsonb", "tsvector":
		if col.Array {
			col.value = new(pq.StringArray)
		} else if col.Nullable {
			col.value = new(sql.NullString)
		} else {
			col.value = new(string)
		}
	case "timestamp without time zone", "timestamp with time zone", "date", "time without time zone", "time with time zone":
		if col.Array {
			// FIXME
			panic("don't know how to bind array column " + col.Name + " of type " + col.Type)
		} else if col.Nullable {
			col.value = new(pq.NullTime)
		} else {
			col.value = new(time.Time)
		}
	case "boolean":
		if col.Array {
			col.value = new([]pq.BoolArray)
		} else if col.Nullable {
			col.value = new(sql.NullBool)
		} else {
			col.value = new(bool)
		}
	case "uuid":
		if col.Array {
			// FIXME
			panic("don't know how to bind array column " + col.Name + " of type " + col.Type)
		} else if col.Nullable {
			// FIXME
			panic("don't know how to bind nullable column " + col.Name + " of type " + col.Type)
		} else {
			col.value = new(uuid.UUID)
		}
	default:
		if col.Array {
			panic("don't know how to bind array column " + col.Name + " of type " + col.Type)
		}
		panic("don't know how to bind column " + col.Name + " of type " + col.Type)
	}
}

func (col column) literal() string {
	if col.value == nil {
		panic("column " + col.Name + " of type " + col.Type + " is not bound")
	}

	switch col.Type {
	case "smallint", "integer", "bigint", "smallserial", "serial", "bigserial":
		if col.Array {
			vs := *col.value.(*pq.Int64Array)
			if len(vs) == 0 {
				return "'{}'"
			}
			literals := make([]string, len(vs))
			for i, x := range vs {
				literals[i] = strconv.FormatInt(x, 10)
			}
			return "ARRAY[" + strings.Join(literals, ", ") + "]"
		}

		var vi64 int64
		if col.Nullable {
			if v := col.value.(*sql.NullInt64); v.Valid {
				vi64 = v.Int64
			} else {
				return "NULL"
			}
		} else {
			vi64 = *col.value.(*int64)
		}
		return strconv.FormatInt(vi64, 10)
	case "real", "double precision":
		if col.Array {
			vs := *col.value.(*pq.Float64Array)
			if len(vs) == 0 {
				return "'{}'"
			}
			literals := make([]string, len(vs))
			for i, x := range vs {
				literals[i] = strconv.FormatFloat(x, 10, -1, 64)
			}
			return "ARRAY[" + strings.Join(literals, ", ") + "]"
		}

		var vf64 float64
		if col.Nullable {
			if v := col.value.(*sql.NullFloat64); v.Valid {
				vf64 = v.Float64
			} else {
				return "NULL"
			}
		} else {
			vf64 = *col.value.(*float64)
		}

		return strconv.FormatFloat(vf64, 'f', -1, 64)
	case "decimal", "numeric", "money", "character varying", "varchar", "character", "char", "text", "binary", "json", "jsonb", "tsvector":
		if col.Array {
			vs := *col.value.(*pq.StringArray)
			if len(vs) == 0 {
				return "'{}'"
			}
			literals := make([]string, len(vs))
			for i, x := range vs {
				literals[i] = pq.QuoteLiteral(x)
			}
			return "ARRAY[" + strings.Join(literals, ", ") + "]"
		}

		var vstr string
		if col.Nullable {
			if v := col.value.(*sql.NullString); v.Valid {
				vstr = v.String
			} else {
				return "NULL"
			}
		} else {
			vstr = *col.value.(*string)
		}

		return pq.QuoteLiteral(vstr)
	case "timestamp without time zone", "timestamp with time zone", "date", "time without time zone", "time with time zone":
		var ts string
		if col.Nullable {
			if v := col.value.(*pq.NullTime); v.Valid {
				ts = v.Time.Format("2006-01-02 15:04:05.000000-07")
			} else {
				return "NULL"
			}
		} else {
			ts = col.value.(*time.Time).Format("2006-01-02 15:04:05.000000-07")
		}

		return pq.QuoteLiteral(ts)
	case "boolean":
		if col.Array {
			vs := *col.value.(*pq.BoolArray)
			if len(vs) == 0 {
				return "'{}'"
			}
			literals := make([]string, len(vs))
			for i, x := range vs {
				literals[i] = strings.ToUpper(strconv.FormatBool(x))
			}
			return "ARRAY[" + strings.Join(literals, ", ") + "]"
		}

		var vb bool
		if col.Nullable {
			if v := col.value.(*sql.NullBool); v.Valid {
				vb = v.Bool
			} else {
				return "NULL"
			}
		} else {
			vb = *col.value.(*bool)
		}

		return strings.ToUpper(strconv.FormatBool(vb))
	case "uuid":
		return pq.QuoteLiteral(col.value.(*uuid.UUID).String())
	default:
		if col.Array {
			panic("don't know how to quote array column " + col.Name + " of type " + col.Type)
		}
		panic("don't know how to quote column " + col.Name + " of type " + col.Type)
	}
}

// getColumns fetches column list for table from database.
func getColumns(q Querier, table string, opts *Options) ([]column, error) {
	rows, err := q.Query(`
		SELECT c.column_name, c.data_type, e.data_type as element_data_type, c.is_nullable
		FROM information_schema.columns c
		LEFT OUTER JOIN information_schema.element_types e
			ON (c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
				= (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier)
		WHERE table_name = $1 AND c.is_generated = 'NEVER'
	`, table)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	cols := make([]column, 0)

	for rows.Next() {
		var col column
		var elementDataType sql.NullString
		var nullable string

		if err := rows.Scan(&col.Name, &col.Type, &elementDataType, &nullable); err != nil {
			return nil, err
		}

		if col.Type == "ARRAY" {
			col.Array = true
			if elementDataType.String == "USER-DEFINED" {
				col.Type = "character varying" // assume character varying
			} else {
				col.Type = elementDataType.String
			}
		} else if col.Type == "USER-DEFINED" {
			col.Type = "character varying" // assume character varying
		}

		if nullable == "YES" {
			col.Nullable = true
		}
		if shouldFetchColumn(col.Name, opts) {
			col.bind()
			cols = append(cols, col)
		}
	}

	return cols, rows.Err()
}

func shouldFetchColumn(name string, opts *Options) bool {
	if len(opts.InsertColumns) == 0 {
		// No columns specified, fetch all!
		return true
	}

	for _, col := range opts.InsertColumns {
		if col == name {
			return true
		}
	}

	return false
}
