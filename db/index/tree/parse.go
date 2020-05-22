package tree

import (
	"encoding/json"
	"fmt"
	"github.com/bidpoc/database-fabric-cc/db/util"
)

type Parse struct {
}

func(parse *Parse) CollectionPointer(value []byte) ([]Pointer, error) {
	collection,err := parse.CollectionBytes(value); if err != nil {
		return nil,err
	}
	var values []Pointer
	if len(collection) > 0 {
		values = make([]Pointer, 0, len(collection))
		for _,v := range collection {
			values = append(values, BytesToPointer(v))
		}
	}
	return values,nil
}

func(parse *Parse) CollectionString(value []byte) ([]string, error) {
	collection,err := parse.CollectionBytes(value); if err != nil {
		return nil,err
	}
	var values []string
	if len(collection) > 0 {
		values = make([]string, 0, len(collection))
		for _,v := range collection {
			values = append(values, fmt.Sprintf("%v",v))
		}
	}
	return values,nil
}

func(parse *Parse) CollectionBytes(value []byte) ([][]byte, error) {
	var collection Collection
	err := json.Unmarshal(value, &collection)
	if err != nil {
		return nil,  fmt.Errorf("parse value to collection error `%s`", err.Error())
	}
	return collection.Values,  nil
}

func(parse *Parse) CollectionFlip(values [][]byte) [][]byte {
	for i,j:= 0,len(values)-1;i<j;i,j=i+1,j-1 {
		values[i],values[j] = values[j],values[i]
	}
	return values
}

func(parse *Parse) BytesByCollectionBytes(value [][]byte) ([]byte, error) {
	collection,err := util.ConvertJsonBytes(Collection{Values:value}); if err != nil {
		return nil,err
	}
	return collection,nil
}