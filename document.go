package jsonb

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Doc struct {
	ID    string
	Attrs interface{}
}

type Docs []Doc

func (d Docs) Values() (string, error) {
	elems := make([]string, 0, len(d))
	for _, doc := range d {
		b, err := json.Marshal(doc.Attrs)
		if err != nil {
			return "", err
		}
		elems = append(elems, fmt.Sprintf("('%s','%s'::jsonb)", doc.ID, string(b)))
	}
	return strings.Join(elems, ","), nil
}
