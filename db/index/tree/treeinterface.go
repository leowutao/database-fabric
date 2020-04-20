package tree

import "gitee.com/bidpoc/database-fabric-cc/db"

type TreeInterface interface {
	CreateHead(key db.ColumnKey, treeType TreeType) (*TreeHead, error)

	SearchHead(key db.ColumnKey) (*TreeHead, error)

	Search(head *TreeHead, key []byte) ([]byte,error)
	SearchByRange(head *TreeHead, startKey []byte, endKey []byte, size Pointer) ([]KV, error)

	Insert(head *TreeHead, key []byte, value []byte, insertType InsertType) error

	Print(head *TreeHead, printData bool) error
}