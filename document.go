package jsonb

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
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
		elems = append(elems, fmt.Sprintf("('%s','%s'::jsonb, '%s')", doc.ID, string(b), time.Now().UTC().Format("2006-01-02 15:04:05-00")))
	}
	return strings.Join(elems, ","), nil
}
