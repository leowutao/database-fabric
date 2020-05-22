package index

import (
	"fmt"
	"github.com/database-fabric/db"
	"github.com/database-fabric/db/index/tree"
	"github.com/database-fabric/db/util"
)

type PrimaryParse struct {
	tree.Parse
}

func(parse *PrimaryParse) BlockIDList(values [][]byte) ([]db.BlockID,error) {
	var blocks []db.BlockID
	if len(values) > 0 {
		blocks = make([]db.BlockID, 0, len(values))
		for _,v := range values {
			blocks = append(blocks, util.BytesToBlockID(parse.ParseBlockID(v)))
		}
	}
	return blocks,nil
}

func(parse *PrimaryParse) BlockID(value []byte) (db.BlockID,error) {
	return util.BytesToBlockID(parse.ParseBlockID(value)),nil
}

func(parse *PrimaryParse) RowBlockIDList(kvList []*db.KV) ([]db.RowBlockID,error) {
	var rowBlockIDList []db.RowBlockID
	if len(kvList) > 0 {
		rowBlockIDList = make([]db.RowBlockID, 0, len(kvList))
		for _,kv := range kvList {
			blockID,err := parse.BlockID(kv.Value); if err != nil {
				return rowBlockIDList,err
			}
			rowBlockIDList = append(rowBlockIDList, db.RowBlockID{RowID:util.BytesToRowID(kv.Key),BlockID:blockID})
		}
	}
	return rowBlockIDList,nil
}

func(parse *PrimaryParse) RowIDList(values [][]byte) ([]db.RowID,error) {
	var rows []db.RowID
	if len(values) > 0 {
		rows = make([]db.RowID, 0, len(values))
		for _,v := range values {
			rows = append(rows, util.BytesToRowID(v))
		}
	}
	return rows,nil
}

//定义一个字节来保存blockID的类型，用字节数组最后一位表示
func(parse *PrimaryParse) FormatBlockType(blockID db.BlockID, op db.OpType) []byte{
	return append(util.BlockIDToBytes(blockID), op)
}

func(parse *PrimaryParse) GetBlockType(value []byte) db.OpType {
	last := len(value) - 1
	return value[last]
}

//定义一个字节来保存blockID的类型，用字节数组最后一位表示
func(parse *PrimaryParse) ParseBlockType(value []byte) ([]byte,db.OpType){
	last := len(value) - 1
	return value[:last],value[last]
}

func(parse *PrimaryParse) ParseBlockID(value []byte) []byte {
	val,_ := parse.ParseBlockType(value)
	return val
}


type PrimaryInsert struct {
	tree.DefaultInsert
	parse *PrimaryParse
}

func NewPrimaryInsertImpl() *PrimaryInsert {
	val := new(PrimaryInsert)
	val.parse = new(PrimaryParse)
	val.SetParse(&val.parse.Parse)
	return val
}

/*
	主键值插入，默认是追加历史block记录
	单独验证行记录删除之后可以新增，未删除触发主键约束
	支持历史版本记录
*/
func(insertImpl *PrimaryInsert) Append(kv *db.KV, oldKV *db.KV) (*tree.RefNode,error) {
	op := insertImpl.parse.GetBlockType(kv.Value)
	refNode,err := insertImpl.DefaultInsert.Append(kv, oldKV); if err != nil {
		return nil,err
	}
	//判定最新的记录是否已删除、未删除返回唯一约束错误
	size := len(refNode.Values)
	if size > 1 {
		if op == db.ADD {
			var lastValue []byte
			if kv.VType == db.ValueTypeLinkedList {
				lastValue = oldKV.Value
			}else{
				lastValue = refNode.Values[size-2]
			}
			lastOp := insertImpl.parse.GetBlockType(lastValue)
			if lastOp != db.DELETE {
				return nil,fmt.Errorf("primary key `%v` is already", kv.Key)
			}
		}
	}
	if kv.VType == db.ValueTypeLinkedList { //链表结构
		kv.Value = refNode.Values[size-1]
	}
	refNode.Update = true
	return refNode,nil
}