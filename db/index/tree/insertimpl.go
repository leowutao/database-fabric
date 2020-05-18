package tree

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
)

type DefaultInsert struct {
	parse *Parse
}

func(insertImpl *DefaultInsert) SetParse(parse *Parse)  {
	if insertImpl.parse == nil {
		insertImpl.parse = parse
	}
}

func(insertImpl *DefaultInsert) GetParse() *Parse {
	if insertImpl.parse == nil {
		insertImpl.parse = new(Parse)
	}
	return insertImpl.parse
}

/*
	默认插入只允许唯一插入
*/
func(insertImpl *DefaultInsert) Default(kv *db.KV, oldKV *db.KV) error {
	if oldKV != nil { //验证唯一性
		return fmt.Errorf("key `%v` is already", kv.Key)
	}
	kv.VType = db.ValueTypeData//默认类型为数据
	return nil
}

/*
	替换原有值,无需验证直接替换
 */
func(insertImpl *DefaultInsert) Replace(kv *db.KV, oldKV *db.KV) error {
	kv.VType = db.ValueTypeData//类型变更为数据
	return nil
}

/*
	插入存在则更新
*/
func(insertImpl *DefaultInsert) Change(kv *db.KV, oldKV *db.KV) error {
	kv.VType = db.ValueTypeData//默认类型为数据
	if oldKV != nil {//存在更新必须符合原值类型
		if oldKV.VType == db.ValueTypeCollection {//只需要验证集合类型
			_,err := insertImpl.GetParse().CollectionBytes(kv.Value); if err != nil {
				return err
			}
			kv.VType = db.ValueTypeCollection
		}
	}
	return nil
}

/*
	在原值追加
	集合元素超过50个会转换为链表结构
*/
func(insertImpl *DefaultInsert) Append(kv *db.KV, oldKV *db.KV) (*RefNode,error) {
	kv.VType = db.ValueTypeCollection //类型变更为集合
	refNode := &RefNode{Update:true,Kv:kv}
	var collection [][]byte
	var err error
	var value []byte
	kvValue := kv.Value
	if oldKV != nil {//原值为集合类型判定容量是否触发转为链表
		if oldKV.VType == db.ValueTypeLinkedList {//链表结构
			kv.VType = db.ValueTypeLinkedList //类型变更为链表
			refNode.Update = false//不更新关键字值
		}else if kv.VType == oldKV.VType {//集合
			oldCollection,err := insertImpl.GetParse().CollectionBytes(oldKV.Value); if err != nil {
				return nil,err
			}
			collection = make([][]byte, 0, len(oldCollection)+1)
			collection = append(collection, oldCollection...)
			num := int64(len(collection))
			if num == 50 {//转为链表结构
				kv.VType = db.ValueTypeLinkedList //类型变更为链表
				kv.Value = nil //值设置为空，此key(索引树叶子节点中)之后不再更新，所有更新操作在链表层
			}
		}else {//单个值
			collection = make([][]byte, 0, 2)
			collection = append(collection, oldKV.Value) //原值需要和当前值组合
		}
	}
	collection = append(collection, kvValue)
	refNode.Values = collection
	if kv.VType == db.ValueTypeCollection {//集合类型格式化
		value,err = insertImpl.GetParse().BytesByCollectionBytes(collection); if err != nil {
			return nil,err
		}
		kv.Value = value
	}
	return refNode,nil
}