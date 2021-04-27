package jsonb

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Database struct {
	pg *pgxpool.Pool
}

func NewDatabase(ctx context.Context, uri string) (*Database, error) {
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	d := &Database{
		pg: pool,
	}
	return d, nil
}

func (d *Database) Table(name string) *Table {
	return &Table{
		name: name,
		pg:   d.pg,
	}
}

// DB returns the underlying pool of connections to use the DB directly
func (d *Database) DB() *pgxpool.Pool {
	return d.pg
}

// Tx returns a new transaction from the DB connection
func (d *Database) Tx(ctx context.Context) (pgx.Tx, error) {
	return d.pg.Begin(ctx)
}

func (d *Database) Close() {
	defer d.pg.Close()
}
