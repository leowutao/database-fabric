package tree

import (
	"github.com/database-fabric/db"
)

type TreeInterface interface {
	CreateHead(key db.ColumnKey, treeType TreeType) (*TreeHead, error)

	SearchHead(key db.ColumnKey) (*TreeHead, error)

	Search(head *TreeHead, key []byte) (*db.KV,error)
	SearchByRange(head *TreeHead, startKey []byte, endKey []byte, order db.OrderType, size Pointer) ([]*db.KV, error)

	Insert(head *TreeHead, key []byte, value []byte, insertType InsertType) (*RefNode,error)

	Print(head *TreeHead, printData bool) error
}

type ValueInterface interface {
	Format(kv *db.KV, oldKV *db.KV, insertType InsertType) (*RefNode,error)
	Parse(kv *db.KV) error
	ToString(value []byte, valueType db.IndexValueType) (string, error)
}

type InsertInterface interface {
	GetParse() *Parse
	Default(kv *db.KV, oldKV *db.KV) error
	Replace(kv *db.KV, oldKV *db.KV) error
	Change(kv *db.KV, oldKV *db.KV) error
	Append(kv *db.KV, oldKV *db.KV) (*RefNode,error)
}