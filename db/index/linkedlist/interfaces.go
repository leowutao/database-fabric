package linkedlist

import "github.com/database-fabric/db"

type LinkedListInterface interface {
	CreateHead(key db.ColumnRowKey) (*LinkedHead, error)

	SearchHead(key db.ColumnRowKey) (*LinkedHead, error)

	SearchByRange(head *LinkedHead, order db.OrderType, size Pointer) ([][]byte,db.Total,error)

	Insert(head *LinkedHead, values [][]byte) error

	Print(head *LinkedHead) error
}