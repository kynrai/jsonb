package jsonb

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v4"
)

func DecodeRows(rows pgx.Rows, results interface{}) error {
	resultsVal := reflect.ValueOf(results)
	if resultsVal.Kind() != reflect.Ptr {
		return fmt.Errorf("results argument must be a pointer to a slice, but was a %s", resultsVal.Kind())
	}
	sliceVal := resultsVal.Elem()
	if sliceVal.Kind() == reflect.Interface {
		sliceVal = sliceVal.Elem()
	}

	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("results argument must be a pointer to a slice, but was a pointer to %s", sliceVal.Kind())
	}
	elementType := sliceVal.Type().Elem()

	var index int
	defer rows.Close()
	for rows.Next() {
		if sliceVal.Len() == index {
			// slice is full
			sliceVal = reflect.Append(sliceVal, reflect.New(elementType).Elem())
			sliceVal = sliceVal.Slice(0, sliceVal.Cap())
		}
		currElem := sliceVal.Index(index).Addr().Interface()
		err := rows.Scan(currElem)
		if err != nil {
			return err
		}
		index++
	}
	resultsVal.Elem().Set(sliceVal.Slice(0, index))
	return nil
}
