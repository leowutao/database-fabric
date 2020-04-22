package tree

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type Pointer = int32  //节点指针类型，占用4byte(节点关键字指向下级指针大小，如果关键字和下级指针类型一致，即占用8byte)
const (
	NODE_PREFIX_MAX_SIZE = 64 	 //节点前缀最大占用空间
	NODE_POINTER_SIZE = 4        //节点指针大小,对应Pointer类型占用空间
	NODE_NAME_SIZE = NODE_PREFIX_MAX_SIZE + NODE_POINTER_SIZE  //节点名字占用空间
)
var NODE_SPLIT_RULE int8 //节点分裂规则：1为关键字个数验证，否则为容量验证

const (
	//容量值配置
	MAX_NODE_SIZE     = 1024 * 4 //节点最大容量4KB
	MAX_KEY_NUM       = 1000     //节点最大key数量，position为int16类型
	MAX_NODE_NUM      = 100000   //树最大节点数量
	MAX_TREE_HEIGHT   = 10        //树最大高度

	//关键字个数配置
	MAX_NODE_KEY_NUM = MAX_TREE_HEIGHT
	MIN_NODE_KEY_NUM = MAX_NODE_KEY_NUM/2
)

type TreeType int8
const (
	TreeTypeDefault TreeType = iota //类型，默认排序为升序，分裂规则1/2
	TreeTypeAsc //排序为升序，分裂规则为升序
	TreeTypeDesc //排序为降序，分裂规则为降序
	TreeTypeDescMid //排序为降序，分裂规则1/2
)

type NodeType int8
const (
	NodeTypeRoot NodeType = iota
	NodeTypeChild
	NodeTypeLeaf
	NodeTypeData
)

type CompareType int8
const (
	CompareLt CompareType = iota
	CompareEq
	CompareGt
)

type InsertType int8
const (
	InsertTypeDefault InsertType = iota //唯一插入
	InsertTypeReplace //插入存在可替换(直接替换原值类型)
	InsertTypeChange //插入存在可更新(必须符合原值类型)
	InsertTypeAppend //插入存在追加插入(集合类型)
)

type ValueType = uint8
const (
	ValueTypeNone ValueType = iota //空
	ValueTypeData //数据
	ValueTypePointer //指针
	ValueTypeCollection //集合
)

//表字段索引树头信息
type TreeHead struct {
	Key 	  db.ColumnKey `json:"key"`    //键
	Type      TreeType `json:"type"`      //树类型
	Root      Pointer  `json:"root"`      //根节点指针
	Height    int8     `json:"height"`    //高度
	NodeOrder Pointer    `json:"nodeOrder"` //当前节点累计自增序号
	NodeNum   Pointer    `json:"nodeNum"`   //节点数量
	KeyNum    int64    `json:"keyNum"`    //关键字数量
	FirstLeaf Pointer  `json:"root"`      //叶子节点链表-头指针
	LastLeaf  Pointer  `json:"root"`      //叶子节点链表-尾指针
}

//节点数据，节点存储标识规则为：索引前缀(前缀+表名+字段名)+排序值(自增)
type TreeNode struct {
	Type   NodeType `json:"type"`   //节点类型，根、子、叶子、数据
	Prev   Pointer `json:"prev"`      //左兄弟节点指针
	Next   Pointer `json:"next"`      //右兄弟节点指针
	Keys   [][]byte `json:"keys"`   //关键字集合
	Values [][]byte `json:"values"` //关键字值集合(与关键字索引位置对应，格式为：指向下级指针或数据内容)
}

type KV struct {
	Key     []byte `json:"key"`
	Value   []byte `json:"value"`
}

type Collection struct {
	Values  [][]byte `json:"values"`
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

func ConvertValue(key []byte, value []byte, oldValue []byte, valueType ValueType, insertType InsertType) ([]byte, error) {
	// TODO value需要判定是否符合原值类型
	var err error
	var newValue interface{}
	var newValueType ValueType
	exists := oldValue != nil
	if insertType == InsertTypeDefault {//默认插入只允许唯一插入
		if exists { //验证唯一性
			return nil, fmt.Errorf("key `%v` is already", key)
		}
		newValueType = ValueTypeData//类型默认为数据
		newValue = value
	} else if insertType == InsertTypeReplace { //替换原有值,无需验证直接替换
		newValueType = ValueTypeData//类型变更为数据
		newValue = value
	} else if insertType == InsertTypeChange { //插入存在则更新
		newValueType = ValueTypeData//默认类型为数据
		if exists {//存在更新必须符合原值类型
			if valueType == ValueTypeCollection {// 只需要验证集合类型
				_,err := ParseValueByType(value, ValueTypeCollection); if err != nil {
					return nil,err
				}
				newValueType = ValueTypeCollection
			}
		}
		newValue = value
	}else if insertType == InsertTypeAppend { //在原值追加
		newValueType = ValueTypeCollection //类型变更为集合
		var collection [][]byte
		if exists {
			if newValueType != valueType { //原值需要和当前值组合
				collection = append(collection, oldValue)
				collection = append(collection, value)
			} else { //原值为集合类型直接追加
				parseOldValue,err := ParseValueByType(oldValue, newValueType); if err != nil {
					return nil,err
				}
				collection = parseOldValue.([][]byte)
				collection = append(collection, value)
			}
		}else{
			collection = append(collection, value)
		}
		newValue = collection
	} else { //类型错误
		return nil, fmt.Errorf("insert type error")
	}
	//转换叶子节点数据结构
	convertValue, err := FormatValueByType(newValue, newValueType)
	if err != nil {
		return nil, err
	}
	return convertValue, nil
}

func ParseCollectionPointer(value []byte) ([]Pointer, error) {
	collection,err := ParseCollectionByte(value); if err != nil {
		return nil,err
	}
	var values []Pointer
	for _,v := range collection {
		values = append(values, BytesToPointer(v))
	}
	return values,nil
}

func ParseCollectionString(value []byte) ([]string, error) {
	collection,err := ParseCollectionByte(value); if err != nil {
		return nil,err
	}
	var values []string
	for _,v := range collection {
		values = append(values, string(v))
	}
	return values,nil
}

func ParseCollectionByte(value []byte) ([][]byte, error) {
	var collection Collection
	err := json.Unmarshal(value, &collection)
	if err != nil {
		return nil,  fmt.Errorf("parse value to collection error `%s`", err.Error())
	}
	return collection.Values,  nil
}

func ParseValueByType(value []byte, valueType ValueType) (interface{}, error) {
	if valueType == ValueTypeData { //数据
		return value, nil
	} else if valueType == ValueTypePointer { //指针
		return BytesToPointer(value), nil
	} else if valueType == ValueTypeCollection { //集合
		return ParseCollectionByte(value)
	}
	return nil, fmt.Errorf("parse value tree.ValueType error")
}

func ParseValueToString(value []byte, valueType ValueType) (string, error) {
	if valueType == ValueTypeData { //数据
		return fmt.Sprintf("%v", value), nil
	} else if valueType == ValueTypePointer { //指针
		return PointerToString(BytesToPointer(value)),nil
	} else if valueType == ValueTypeCollection { //集合
		collection,err := ParseCollectionString(value); if err != nil {
			return "",err
		}
		return util.ConvertJsonString(collection)
	}
	return "", fmt.Errorf("parse value to string tree.ValueType error")
}

func FormatValueByType(value interface{}, valueType ValueType) ([]byte, error) {
	var convertValue []byte
	if valueType == ValueTypeData { //数据
		convertValue = value.([]byte)
	} else if valueType == ValueTypePointer { //指针
		convertValue = PointerToBytes(value.(Pointer))
	} else if valueType == ValueTypeCollection { //集合
		var err error
		convertValue,err = util.ConvertJsonBytes(Collection{value.([][]byte)}); if err != nil {
			return nil,err
		}
	} else {
		return nil, fmt.Errorf("format node value tree.ValueType error")
	}
	return append(convertValue, valueType), nil
}