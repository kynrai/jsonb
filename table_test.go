package jsonb_test

import (
	"context"
	"sort"
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
	table := db.Table("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	table.DeleteMany(ctx, jsonb.F{})

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

	table := db.Table("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	table.DeleteMany(ctx, jsonb.F{})

	doc1 := &Doc{ID: uuid.NewString(), Name: "tester1", Age: 10, Location: "UK"}
	doc2 := &Doc{ID: uuid.NewString(), Name: "tester2", Age: 20, Location: "US"}
	doc3 := &Doc{ID: uuid.NewString(), Name: "tester3", Age: 30, Location: "FR"}
	doc4 := &Doc{ID: uuid.NewString(), Name: "tester5", Age: 40, Location: "DE"}
	doc5 := &Doc{ID: uuid.NewString(), Name: "tester1", Age: 40, Location: "DE"}

	docs := []*Doc{doc1, doc2, doc3, doc4, doc5}
	for _, doc := range docs {
		_, err = table.InsertByID(ctx, doc.ID, doc)
		assert.Nil(t, err)
	}

	for _, tc := range []struct {
		name     string
		filter   jsonb.F
		expected []*Doc
		opts     []jsonb.FilterOption
	}{
		{
			name: "find both tester1",
			filter: jsonb.F{
				{"name", "tester1"},
			},
			expected: []*Doc{doc1, doc5},
		},
		{
			name: "find DE or UK",
			filter: jsonb.F{
				{"location", []string{"UK", "DE"}},
			},
			expected: []*Doc{doc1, doc4, doc5},
		},
		{
			name: "find DE or UK tester1",
			filter: jsonb.F{
				{"name", "tester1"},
				{"location", []string{"UK", "DE"}},
			},
			expected: []*Doc{doc1, doc5},
		},
		{
			name: "find ages 10,20",
			filter: jsonb.F{
				{"age", []int{10, 20}},
			},
			expected: []*Doc{doc1, doc2},
		},
		{
			name: "find UK tester 1 ages 10,20, UK DE",
			filter: jsonb.F{
				{"name", "tester1"},
				{"age", []int{10, 20}},
				{"location", []string{"UK", "DE"}},
			},
			expected: []*Doc{doc1},
		},
		{
			name:     "find everything",
			filter:   jsonb.F{},
			expected: []*Doc{doc1, doc2, doc3, doc4, doc5},
		},
		{
			name:     "find everything limit 2",
			filter:   jsonb.F{},
			opts:     []jsonb.FilterOption{jsonb.Limit(2)},
			expected: []*Doc{doc1, doc2},
		},
		{
			name:     "find everything limit 2 offset 2",
			filter:   jsonb.F{},
			opts:     []jsonb.FilterOption{jsonb.Limit(2), jsonb.Offset(2)},
			expected: []*Doc{doc3, doc4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := table.Find(ctx, tc.filter, tc.opts...)
			assert.Nil(t, err)
			resp := []*Doc{}
			err = jsonb.DecodeRows(rows, &resp)
			assert.Nil(t, err)
			sort.SliceStable(resp, func(i, j int) bool {
				return resp[i].Age < resp[j].Age
			})

			assert.Equal(t, len(tc.expected), len(resp), "unexpected number of results")

			for i, doc := range tc.expected {
				assert.Equal(t, doc, resp[i])
			}
		})
	}
}

func TestUpdateByID(t *testing.T) {
	uri := "postgresql://user:password@localhost:5432/ams?sslmode=disable"
	ctx := context.Background()
	db, err := jsonb.NewDatabase(ctx, uri)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	table := db.Table("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	table.DeleteMany(ctx, jsonb.F{})

	doc1 := Doc{ID: uuid.NewString(), Name: "tester1"}
	_, err = table.InsertByID(ctx, doc1.ID, doc1)
	assert.Nil(t, err)

	var resp Doc
	err = table.FindByID(ctx, doc1.ID, &resp)
	assert.Nil(t, err)

	resp.Name = "tester-updated"
	_, err = table.UpdateByID(ctx, resp.ID, resp)
	assert.Nil(t, err)

	var resp2 Doc
	err = table.FindByID(ctx, doc1.ID, &resp2)
	assert.Nil(t, err)

	assert.Equal(t, resp, resp2)
}

func TestDeleteByID(t *testing.T) {
	uri := "postgresql://user:password@localhost:5432/ams?sslmode=disable"
	ctx := context.Background()
	db, err := jsonb.NewDatabase(ctx, uri)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	table := db.Table("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	table.DeleteMany(ctx, jsonb.F{})

	doc1 := Doc{ID: uuid.NewString(), Name: "tester1"}
	_, err = table.InsertByID(ctx, doc1.ID, doc1)
	assert.Nil(t, err)

	var resp Doc
	err = table.FindByID(ctx, doc1.ID, &resp)
	assert.Nil(t, err)

	assert.Equal(t, doc1, resp)

	_, err = table.DeleteByID(ctx, doc1.ID)
	assert.Nil(t, err)

	err = table.FindByID(ctx, doc1.ID, &resp)
	assert.Equal(t, "no rows in result set", err.Error())
}

func TestCount(t *testing.T) {
	uri := "postgresql://user:password@localhost:5432/ams?sslmode=disable"
	ctx := context.Background()
	db, err := jsonb.NewDatabase(ctx, uri)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	table := db.Table("test")
	_, err = table.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	table.DeleteMany(ctx, jsonb.F{})

	doc1 := &Doc{ID: uuid.NewString(), Name: "tester1", Age: 10, Location: "UK"}
	doc2 := &Doc{ID: uuid.NewString(), Name: "tester2", Age: 20, Location: "US"}
	doc3 := &Doc{ID: uuid.NewString(), Name: "tester3", Age: 30, Location: "FR"}
	doc4 := &Doc{ID: uuid.NewString(), Name: "tester5", Age: 40, Location: "DE"}
	doc5 := &Doc{ID: uuid.NewString(), Name: "tester1", Age: 40, Location: "DE"}

	docs := []*Doc{doc1, doc2, doc3, doc4, doc5}
	for _, doc := range docs {
		_, err = table.InsertByID(ctx, doc.ID, doc)
		assert.Nil(t, err)
	}

	t.Run("count everything", func(t *testing.T) {
		count, err := table.CountDocuments(ctx, jsonb.F{})
		assert.Nil(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("count tester1s", func(t *testing.T) {
		count, err := table.CountDocuments(ctx, jsonb.F{
			{"name", "tester1"},
		})
		assert.Nil(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("count filter", func(t *testing.T) {
		count, err := table.CountDocuments(ctx, jsonb.F{
			{"name", "tester1"},
			{"age", []int{10, 20}},
			{"location", []string{"UK", "DE"}},
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, count)
	})

}
