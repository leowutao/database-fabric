package linkedlist

import (
	"github.com/database-fabric/db"
	"github.com/database-fabric/db/util"
)

type Pointer = int32  //节点指针类型，占用4byte(节点关键字指向下级指针大小，如果关键字和下级指针类型一致，即占用8byte)
const (
	NODE_PREFIX_MAX_SIZE = 64 	 //节点前缀最大占用空间
	NODE_POINTER_SIZE = 4        //节点指针大小,对应Pointer类型占用空间
	NODE_NAME_SIZE = NODE_PREFIX_MAX_SIZE + NODE_POINTER_SIZE  //节点名字占用空间
)

const (
	//容量值配置
	MAX_NODE_SIZE     = 1024 * 4 //节点最大容量4KB
)

type LinkedHead struct {
	Key   db.ColumnRowKey `json:"key"`    //键
	Order  Pointer    `json:"order"` //当前节点累计自增序号
	Num   int64    `json:"num"` //值列表总数
	First Pointer  `json:"first"`   //头指针
	Last  Pointer  `json:"last"`    //尾指针
}

type LinkedNode struct {
	Prev   Pointer `json:"prev"`      //左兄弟节点指针
	Next   Pointer `json:"next"`      //右兄弟节点指针
	Values [][]byte `json:"values"`   //值集合
}

func BytesToPointer(value []byte) Pointer {
	return util.BytesToInt32(value)
}

func PointerToBytes(pointer Pointer) []byte {
	return util.Int32ToBytes(pointer)
}

func PointerToString(pointer Pointer) string {
	return util.Int64ToString(int64(pointer))
}