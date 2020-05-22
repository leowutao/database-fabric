package tree

import (
	"fmt"
	"github.com/database-fabric/db"
)

type DefaultValue struct {
	iInsert *InsertInterface
}

func NewDefaultValue(iInsert *InsertInterface) ValueInterface {
	val := new(DefaultValue)
	val.iInsert = iInsert
	return val
}

func(valueImpl *DefaultValue) Format(kv *db.KV, oldKV *db.KV, insertType InsertType) (*RefNode,error){
	var err error
	var refNode *RefNode
	switch insertType {
		case InsertTypeDefault:
			err = (*valueImpl.iInsert).Default(kv, oldKV)
		case InsertTypeReplace:
			err = (*valueImpl.iInsert).Replace(kv, oldKV)
		case InsertTypeChange:
			err = (*valueImpl.iInsert).Change(kv, oldKV)
		case InsertTypeAppend:
			refNode,err = (*valueImpl.iInsert).Append(kv, oldKV)
		default:
			err = fmt.Errorf("insert type error")
	}
	if err != nil {
		return nil,err
	}
	kv.Value = append(kv.Value, kv.VType)
	return refNode,nil
}

func(valueImpl *DefaultValue) Parse(kv *db.KV) error{
	//定义一个字节来保存value的类型，用字节数组最后一位表示
	last := len(kv.Value) - 1
	kv.VType = kv.Value[last]
	kv.Value = kv.Value[:last]
	return nil
}

func(valueImpl *DefaultValue) ToString(value []byte, valueType db.IndexValueType) (string, error) {
	if valueType == db.ValueTypeData { //数据
		return fmt.Sprintf("%v", value), nil
	} else if valueType == db.ValueTypePointer { //指针
		return PointerToString(BytesToPointer(value)),nil
	} else if valueType == db.ValueTypeCollection { //集合
		collection,err := (*valueImpl.iInsert).GetParse().CollectionString(value); if err != nil {
			return "",err
		}
		return fmt.Sprintf("%v",collection),nil
	}
	return "", fmt.Errorf("parse value to string tree.ValueType error")
}