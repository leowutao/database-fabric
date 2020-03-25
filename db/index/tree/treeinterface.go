package tree

type TreeInterface interface {
	Search(table string, column string, key []byte) (interface{},ValueType,error)

	Insert(table string, column string, key []byte, value interface{}, insertType InsertType) error

	Print(table string, column string) error
}