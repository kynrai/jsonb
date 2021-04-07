package jsonb

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Table struct {
	name string
	pg   *pgxpool.Pool
}

const sqlCreateTable = `
CREATE TABLE IF NOT EXISTS %[1]s (
    id UUID PRIMARY KEY,
    attrs JSONB
);
-- Create an index on all key/value pairs in the JSONB column.
CREATE INDEX IF NOT EXISTS idx_%[1]s_attrs ON %[1]s USING gin (attrs);
`

// Create the table if it does not already exist
func (t *Table) Create(ctx context.Context) (pgconn.CommandTag, error) {
	return t.pg.Exec(ctx, fmt.Sprintf(sqlCreateTable, t.name))
}

const sqlInsertByID = `
INSERT INTO %s (id,attrs) VALUES ($1,$2);
`

// InsertByID document into the table with the given ID
func (t *Table) InsertByID(ctx context.Context, id string, doc interface{}) (pgconn.CommandTag, error) {
	return t.pg.Exec(ctx, fmt.Sprintf(sqlInsertByID, t.name), id, doc)
}

func (t *Table) InsertMany(ctx context.Context) error {
	return nil
}

const sqlFindByID = `
SELECT attrs FROM %s WHERE id = $1;
`

// FindByID returns selects a document with the given ID and unmarhsals the data into v.
// v must be a pointer to a struct which represents the document being returned
func (t *Table) FindByID(ctx context.Context, id string, v interface{}) error {
	return t.pg.QueryRow(ctx, fmt.Sprintf(sqlFindByID, t.name), id).Scan(v)
}

func (t *Table) FindOne(ctx context.Context) error {
	return nil
}

const sqlFind = `
SELECT attrs FROM %s %s;
`

func (t *Table) Find(ctx context.Context, filter F) (pgx.Rows, error) {
	where, err := filter.Where()
	if err != nil {
		return nil, err
	}
	return t.pg.Query(ctx, fmt.Sprintf(sqlFind, t.name, where))
}

func (t *Table) UpdateByID(ctx context.Context) error {
	return nil
}

func (t *Table) UpdateMany(ctx context.Context) error {
	return nil
}

func (t *Table) DeleteOne(ctx context.Context) error {
	return nil
}

func (t *Table) DeleteMany(ctx context.Context) error {
	return nil
}

func (t *Table) CountDocuments(ctx context.Context) error {
	return nil
}
