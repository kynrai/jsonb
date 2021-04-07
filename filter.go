package jsonb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type KV struct {
	Key   string
	Value interface{}
}

type F []KV

// Map creates a map from the elements of the F
func (f F) Map() map[string]interface{} {
	m := make(map[string]interface{}, len(f))
	for _, e := range f {
		m[e.Key] = e.Value
	}
	return m
}

// Where generate the where clause of the SQL statement
func (f F) Where() (string, error) {
	clause := []string{}
	m := f.Map()
	for key, val := range m {
		v := reflect.ValueOf(val)
		switch v.Kind() {
		case reflect.Slice:
			elements := make([]string, 0, v.Len())
			for i := 0; i < v.Len(); i++ {
				elements = append(elements, fmt.Sprintf("'%v'", v.Index(i).Interface()))
			}
			clause = append(clause, fmt.Sprintf("attrs->>'%s' IN (%v)", key, strings.Join(elements, ",")))
		default:
			j := make(map[string]interface{})
			j[key] = val
			b, err := json.Marshal(j)
			if err != nil {
				return "", err
			}
			clause = append(clause, fmt.Sprintf("attrs @> '%s'::jsonb", string(b)))
		}
	}
	return fmt.Sprint("WHERE ", strings.Join(clause, " AND ")), nil
}
