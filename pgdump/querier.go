package pgdump

import (
	"database/sql"

	"golang.org/x/net/context"
)

type Querable interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type Querier struct {
	ctx context.Context
	q   Querable
}

func NewQuerier(q Querable) Querier {
	return NewQuerierContext(context.Background(), q)
}

func NewQuerierContext(ctx context.Context, q Querable) Querier {
	return Querier{
		ctx: ctx,
		q:   q,
	}
}

func (q Querier) Exec(query string, args ...interface{}) (sql.Result, error) {
	return q.q.ExecContext(q.ctx, query, args...)
}

func (q Querier) Prepare(query string) (*sql.Stmt, error) {
	return q.q.PrepareContext(q.ctx, query)
}

func (q Querier) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.q.QueryContext(q.ctx, query, args...)
}

func (q Querier) QueryRow(query string, args ...interface{}) *sql.Row {
	return q.q.QueryRowContext(q.ctx, query, args...)
}
