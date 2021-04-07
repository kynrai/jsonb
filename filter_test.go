package jsonb_test

import (
	"fmt"
	"testing"

	"github.com/kynrai/jsonb"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	filter := jsonb.F{{"name", "tester1"}, {"age", []int{10, 20}}, {"location", []string{"UK", "US"}}}
	where, err := filter.Where()
	assert.Nil(t, err)
	fmt.Println("SELECT attrs FROM", where)
}
