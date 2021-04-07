package jsonb

import (
	"context"

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

func (d *Database) NewTable(name string) *Table {
	return &Table{
		name: name,
		pg:   d.pg,
	}
}
