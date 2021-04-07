package jsonb_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/kynrai/jsonb"
	"github.com/stretchr/testify/assert"
)

type Doc struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Age      int    `json:"age,omitempty"`
	Location string `json:"location,omitempty"`
}

func TestInsertAndFindByID(t *testing.T) {
	uri := "postgresql://user:password@localhost:5432/ams?sslmode=disable"
	ctx := context.Background()
	db, err := jsonb.NewDatabase(ctx, uri)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	table := db.NewTable("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// test a normal document
	doc1 := Doc{
		ID:   uuid.NewString(),
		Name: "tester1",
	}
	_, err = table.InsertByID(ctx, doc1.ID, doc1)
	assert.Nil(t, err)

	var resp Doc
	err = table.FindByID(ctx, doc1.ID, &resp)
	assert.Nil(t, err)

	assert.Equal(t, doc1, resp)

	// test a pointer to a document

	doc2 := &Doc{
		ID:   uuid.NewString(),
		Name: "tester2",
	}
	_, err = table.InsertByID(ctx, doc2.ID, doc2)
	assert.Nil(t, err)

	var resp2 Doc
	err = table.FindByID(ctx, doc2.ID, &resp2)
	assert.Nil(t, err)

	assert.Equal(t, doc2, &resp2)
}

func TestInsertAndFind(t *testing.T) {
	uri := "postgresql://user:password@localhost:5432/ams?sslmode=disable"
	ctx := context.Background()
	db, err := jsonb.NewDatabase(ctx, uri)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	table := db.NewTable("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// test a normal document
	docs := []*Doc{
		{ID: uuid.NewString(), Name: "tester1", Age: 10, Location: "UK"},
		{ID: uuid.NewString(), Name: "tester2", Age: 20, Location: "US"},
		{ID: uuid.NewString(), Name: "tester3", Age: 10, Location: "FR"},
		{ID: uuid.NewString(), Name: "tester1", Age: 40, Location: "DE"},
	}
	for _, doc := range docs {
		_, err = table.InsertByID(ctx, doc.ID, doc)
		assert.Nil(t, err)
	}
	rows, err := table.Find(ctx, jsonb.F{
		{"name", "tester1"},
		{"age", []int{10, 20, 40}},
		{"location", []string{"UK", "US"}},
	})
	assert.Nil(t, err)
	resp := []*Doc{}
	err = jsonb.DecodeRows(rows, &resp)
	assert.Nil(t, err)

	for _, res := range resp {
		t.Log(res)
	}
}
