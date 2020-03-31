package tree

type TreeInterface interface {
	CreateHead(table string, column string, treeType TreeType) (*TreeHead, error)

	SearchHead(table string, column string) (*TreeHead, error)

	Search(head *TreeHead, key []byte) ([]byte,error)
	SearchByRange(head *TreeHead, startKey []byte, endKey []byte, size Pointer) ([]KV, error)

	Insert(head *TreeHead, key []byte, value []byte, insertType InsertType) error

	Print(head *TreeHead, printData bool) error
}