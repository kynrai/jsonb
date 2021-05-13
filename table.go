package jsonb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type PKType string

const (
	PK_UUID PKType = "UUID"
	PK_TEXT PKType = "TEXT"
)

type Table struct {
	PKType PKType
	name   string
	pg     *pgxpool.Pool
}

const sqlCreateTable = `
CREATE TABLE IF NOT EXISTS %[1]s (
    id %[2]s PRIMARY KEY,
    attrs JSONB,
	updatedAt TIMESTAMPTZ
);
-- Create an index on all key/value pairs in the JSONB column.
CREATE INDEX IF NOT EXISTS idx_%[1]s_attrs ON %[1]s USING gin (attrs);
`

// Create the table if it does not already exist
func (t *Table) Create(ctx context.Context) (pgconn.CommandTag, error) {
	tx := TxFromContext(ctx)
	if t.PKType == "" {
		t.PKType = PK_UUID
	}
	if tx != nil {
		return tx.Exec(ctx, fmt.Sprintf(sqlCreateTable, t.name, t.PKType))
	}
	return t.pg.Exec(ctx, fmt.Sprintf(sqlCreateTable, t.name, t.PKType))
}

const sqlInsertByID = `
INSERT INTO %s (id,attrs,updatedAt) VALUES ($1,$2,$3);
`

// InsertByID document into the table with the given ID
func (t *Table) InsertByID(ctx context.Context, id string, doc interface{}) (pgconn.CommandTag, error) {
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.Exec(ctx, fmt.Sprintf(sqlInsertByID, t.name), id, doc, time.Now().UTC())
	}
	return t.pg.Exec(ctx, fmt.Sprintf(sqlInsertByID, t.name), id, doc, time.Now().UTC())
}

const sqlInsertMany = `
INSERT INTO %s (id,attrs,updatedAt) VALUES %s;
`

// InsertMany documents into the table
func (t *Table) InsertMany(ctx context.Context, docs Docs) (pgconn.CommandTag, error) {
	valStr, err := docs.Values()
	if err != nil {
		return nil, err
	}
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.Exec(ctx, fmt.Sprintf(sqlInsertMany, t.name, valStr))
	}
	return t.pg.Exec(ctx, fmt.Sprintf(sqlInsertMany, t.name, valStr))
}

const sqlFindByID = `
SELECT attrs FROM %s WHERE id = $1;
`

// FindByID returns selects a document with the given ID and unmarhsals the data into v.
// v must be a pointer to a struct which represents the document being returned
func (t *Table) FindByID(ctx context.Context, id string, v interface{}) error {
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.QueryRow(ctx, fmt.Sprintf(sqlFindByID, t.name), id).Scan(v)
	}
	return t.pg.QueryRow(ctx, fmt.Sprintf(sqlFindByID, t.name), id).Scan(v)
}

const sqlFind = `
SELECT attrs FROM %s %s;
`

// Find applies a filter and returns rows
func (t *Table) Find(ctx context.Context, filter F, opts ...FilterOption) (pgx.Rows, error) {
	where, err := filter.Where()
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		where += " " + opt(&filter)
	}
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.Query(ctx, fmt.Sprintf(sqlFind, t.name, where))
	}
	return t.pg.Query(ctx, fmt.Sprintf(sqlFind, t.name, where))
}

const sqlUpdateByID = `
UPDATE %s SET attrs = $2, updatedAt = $3 WHERE id = $1;
`

// UpdateByID updates a doc with the given ID, this does a full replace of the existing document
func (t *Table) UpdateByID(ctx context.Context, id string, doc interface{}) (pgconn.CommandTag, error) {
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.Exec(ctx, fmt.Sprintf(sqlUpdateByID, t.name), id, doc, time.Now().UTC())
	}
	return t.pg.Exec(ctx, fmt.Sprintf(sqlUpdateByID, t.name), id, doc, time.Now().UTC())
}

const sqlDeleteByID = `
DELETE from %s WHERE id = $1;
`

// DeleteByID deletes a document with the given ID
func (t *Table) DeleteByID(ctx context.Context, id string) (pgconn.CommandTag, error) {
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.Exec(ctx, fmt.Sprintf(sqlDeleteByID, t.name), id)
	}
	return t.pg.Exec(ctx, fmt.Sprintf(sqlDeleteByID, t.name), id)
}

const sqlDelete = `
DELETE from %s %s;
`

// DeleteMany documents by a filter, empty filter deletes all documents
func (t *Table) DeleteMany(ctx context.Context, filter F) (pgconn.CommandTag, error) {
	where, err := filter.Where()
	if err != nil {
		return nil, err
	}
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx.Exec(ctx, fmt.Sprintf(sqlDelete, t.name, where))
	}
	return t.pg.Exec(ctx, fmt.Sprintf(sqlDelete, t.name, where))
}

const sqlCount = `
SELECT count(*) AS count FROM %s %s;
`

// CountDocuments applies a fitler and counts the results, empty filter counts all documents
func (t *Table) CountDocuments(ctx context.Context, filter F) (int, error) {
	where, err := filter.Where()
	if err != nil {
		return 0, err
	}
	var count int
	tx := TxFromContext(ctx)
	if tx != nil {
		return count, tx.QueryRow(ctx, fmt.Sprintf(sqlCount, t.name, where)).Scan(&count)
	}
	return count, t.pg.QueryRow(ctx, fmt.Sprintf(sqlCount, t.name, where)).Scan(&count)
}
