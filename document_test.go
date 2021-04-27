package jsonb_test

import (
	"testing"

	"github.com/kynrai/jsonb"
	"github.com/stretchr/testify/assert"
)

type doc struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Age  int    `json:"age,omitempty"`
}

func TestDocsValues(t *testing.T) {
	t.Skip("range on time and compare structs instead")
	doc1 := doc{ID: "16b06253-43e0-4354-959f-1885f431c7f0", Name: "tester1", Age: 10}
	doc2 := doc{ID: "8393854c-98ae-4ebc-bd32-f1ea8291fafe", Name: "tester1", Age: 10}

	docs := jsonb.Docs{
		{ID: doc1.ID, Attrs: doc1},
		{ID: doc2.ID, Attrs: doc2},
	}
	str, err := docs.Values()
	if err != nil {
		t.Fatalf("could not create doc values: %v", err)
	}
	expected := `('16b06253-43e0-4354-959f-1885f431c7f0','{"id":"16b06253-43e0-4354-959f-1885f431c7f0","name":"tester1","age":10}'::jsonb),('8393854c-98ae-4ebc-bd32-f1ea8291fafe','{"id":"8393854c-98ae-4ebc-bd32-f1ea8291fafe","name":"tester1","age":10}'::jsonb)`
	assert.Equal(t, expected, str)
}
