package bptree

import (
	"fmt"
	"github.com/bidpoc/database-fabric-cc/db"
	"github.com/bidpoc/database-fabric-cc/db/index/tree"
	"github.com/bidpoc/database-fabric-cc/db/storage"
	"github.com/bidpoc/database-fabric-cc/db/storage/state"
	"github.com/bidpoc/database-fabric-cc/db/util"
	"github.com/bidpoc/database-fabric-cc/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBPTree(t *testing.T){
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	var iInsert tree.InsertInterface = new(tree.DefaultInsert)
	bPTreeImpl := NewBPTreeImpl(storage.NewBPTreeStorage(state), tree.NewDefaultValue(&iInsert))

	//树基本验证
	{
		key := db.ColumnKey{Database:db.DatabaseID(1),Table:db.TableID(1),Column:db.ColumnID(1)}
		treeHead,err := bPTreeImpl.CreateHead(key, tree.TreeTypeAsc); if err != nil {
			panic(err.Error())
		}
		start := tree.Pointer(1)
		size := tree.Pointer(1000)
		for i:=start; i <= size; i++ {
			v := tree.PointerToBytes(i)
			if _,err := bPTreeImpl.Insert(treeHead, v, v, tree.InsertTypeDefault); err != nil {
				fmt.Println(i)
				panic(err.Error())
			}
		}
		if err := bPTreeImpl.Print(treeHead,true); err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, treeHead.NodeOrder, treeHead.NodeNum,"head node num error")
		assert.EqualValues(t, treeHead.LastLeaf, treeHead.NodeOrder,"head lastLeaf error")

		//全表扫描-升序查询
		list, err := bPTreeImpl.SearchByRange(treeHead,nil,nil, db.ASC, size); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, len(list), size,"key num error")
		assert.EqualValues(t, list[0].Key, tree.PointerToBytes(start),"start key error")
		assert.EqualValues(t, list[0].Value, tree.PointerToBytes(start),"start value error")
		assert.EqualValues(t, list[0].VType, db.ValueTypeData,"start type error")
		assert.EqualValues(t, list[len(list)-1].Key, tree.PointerToBytes(size),"end key error")
		assert.EqualValues(t, list[len(list)-1].Value, tree.PointerToBytes(size),"end value error")
		assert.EqualValues(t, list[len(list)-1].VType, db.ValueTypeData,"end type error")
		//全表扫描-降序查询
		list, err = bPTreeImpl.SearchByRange(treeHead,nil,nil, db.DESC, size); if err != nil {
		panic(err.Error())
	}
		assert.EqualValues(t, len(list), size,"key num error")
		assert.EqualValues(t, list[0].Key, tree.PointerToBytes(size),"start key error")
		assert.EqualValues(t, list[0].Value, tree.PointerToBytes(size),"start value error")
		assert.EqualValues(t, list[0].VType, db.ValueTypeData,"start type error")
		assert.EqualValues(t, list[len(list)-1].Key, tree.PointerToBytes(start),"end key error")
		assert.EqualValues(t, list[len(list)-1].Value, tree.PointerToBytes(start),"end value error")
		assert.EqualValues(t, list[len(list)-1].VType, db.ValueTypeData,"end type error")
		//条件查询-升序查询
		startKey := tree.PointerToBytes(100)
		endKey := tree.PointerToBytes(500)
		pageSize := tree.Pointer(401)
		list, err = bPTreeImpl.SearchByRange(treeHead, startKey, endKey, db.ASC, pageSize); if err != nil {
		panic(err.Error())
	}
		assert.EqualValues(t, len(list), pageSize,"key num error")
		assert.EqualValues(t, list[0].Key, startKey,"start key error")
		assert.EqualValues(t, list[0].Value, startKey,"start value error")
		assert.EqualValues(t, list[0].VType, db.ValueTypeData,"start type error")
		assert.EqualValues(t, list[len(list)-1].Key, endKey,"end key error")
		assert.EqualValues(t, list[len(list)-1].Value, endKey,"end value error")
		assert.EqualValues(t, list[len(list)-1].VType, db.ValueTypeData,"end type error")
		//条件查询-降序查询
		startKey = tree.PointerToBytes(500)
		endKey = tree.PointerToBytes(100)
		pageSize = tree.Pointer(401)
		list, err = bPTreeImpl.SearchByRange(treeHead, startKey, endKey, db.DESC, pageSize); if err != nil {
		panic(err.Error())
	}
		assert.EqualValues(t, len(list), pageSize,"key num error")
		assert.EqualValues(t, list[0].Key, startKey,"start key error")
		assert.EqualValues(t, list[0].Value, startKey,"start value error")
		assert.EqualValues(t, list[0].VType, db.ValueTypeData,"start type error")
		assert.EqualValues(t, list[len(list)-1].Key, endKey,"end key error")
		assert.EqualValues(t, list[len(list)-1].Value, endKey,"end value error")
		assert.EqualValues(t, list[len(list)-1].VType, db.ValueTypeData,"end type error")
		//分页查询-升序查询
		pageSize = tree.Pointer(15)
		startKey = tree.PointerToBytes(start)
		endKey = tree.PointerToBytes(pageSize)
		list, err = bPTreeImpl.SearchByRange(treeHead, startKey, endKey, db.ASC, pageSize); if err != nil {
		panic(err.Error())
	}
		assert.EqualValues(t, len(list), pageSize,"key num error")
		assert.EqualValues(t, list[0].Key, startKey,"start key error")
		assert.EqualValues(t, list[0].Value, startKey,"start value error")
		assert.EqualValues(t, list[0].VType, db.ValueTypeData,"start type error")
		assert.EqualValues(t, list[len(list)-1].Key, endKey,"end key error")
		assert.EqualValues(t, list[len(list)-1].Value, endKey,"end value error")
		assert.EqualValues(t, list[len(list)-1].VType, db.ValueTypeData,"end type error")
		//分页查询-降序查询
		pageSize = tree.Pointer(15)
		startKey = tree.PointerToBytes(size)
		endKey = tree.PointerToBytes(size-pageSize+1)
		list, err = bPTreeImpl.SearchByRange(treeHead, startKey, endKey, db.DESC, pageSize); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, len(list), pageSize,"key num error")
		assert.EqualValues(t, list[0].Key, startKey,"start key error")
		assert.EqualValues(t, list[0].Value, startKey,"start value error")
		assert.EqualValues(t, list[0].VType, db.ValueTypeData,"start type error")
		assert.EqualValues(t, list[len(list)-1].Key, endKey,"end key error")
		assert.EqualValues(t, list[len(list)-1].Value, endKey,"end value error")
		assert.EqualValues(t, list[len(list)-1].VType, db.ValueTypeData,"end type error")
		//精确查询-存在
		findKey := tree.PointerToBytes(678)
		kv,err := bPTreeImpl.Search(treeHead, findKey); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, kv.Key, findKey,"find key error")
		assert.EqualValues(t, kv.Value, findKey,"find value error")
		assert.EqualValues(t, kv.VType, db.ValueTypeData,"find type error")
		//精确查询-不存在
		findKey = tree.PointerToBytes(-1)
		kv,err = bPTreeImpl.Search(treeHead, findKey); if err != nil {
		panic(err.Error())
	}
		assert.Nil(t, kv,"find key error")

		//统计树高，key总数
		//cache,err := createTreeNodeCache(treeHead,false); if err != nil {
		//	panic(err.Error())
		//}
		//keyNum := 0
		//for i:=tree.Pointer(1);i<=treeHead.NodeNum;i++{
		//	nodePosition,err := bPTreeImpl.getNodePosition(i,nil,0, cache); if err != nil {
		//		panic(err.Error())
		//	}
		//	keyNum += len(nodePosition.Node.Keys)
		//	//if nodePosition.Node.Type == nodetype
		//}
		fmt.Println(util.ConvertJsonString(*treeHead))
	}


	//主键：顺序插入1000条
	//{
	//	table := "User"
	//	column := "id"
	//	treeHead,err := bPTreeImpl.CreateHead(table, column, tree.TreeTypeAsc); if err != nil {
	//		panic(err.Error())
	//	}
	//	for i := tree.Pointer(1); i <= 1000; i++ {
	//		v := tree.PointerToBytes(i)
	//		if err := bPTreeImpl.Insert(treeHead, v, v, tree.InsertTypeDefault); err != nil {
	//			fmt.Println(i)
	//			panic(err.Error())
	//		}
	//	}
	//	assert.EqualValues(t, treeHead.KeyNum>1000,true,"tree head key num error")
	//	list, err := bPTreeImpl.SearchByRange(treeHead, tree.PointerToBytes(1), tree.PointerToBytes(1000),1000); if err != nil {
	//		panic(err.Error())
	//	}
	//	assert.EqualValues(t, len(list),1000,"key num error")
	//	assert.EqualValues(t, list[0].Key,tree.PointerToBytes(1),"key first error")
	//	assert.EqualValues(t, list[len(list)-1].Key,tree.PointerToBytes(1000),"key last error")
	//
	//	value, err := bPTreeImpl.Search(treeHead, tree.PointerToBytes(899)); if err != nil {
	//		panic(err.Error())
	//	}
	//	assert.EqualValues(t, tree.BytesToPointer(value),899,"search failed")
	//}

	//外建：随机插入10000条
	//{
	//	table := "ShoppingCart"
	//	column := "userId"
	//	treeHead,err := bPTreeImpl.CreateHead(table, column, tree.TreeTypeDefault); if err != nil {
	//		panic(err.Error())
	//	}
	//	for i := tree.Pointer(1); i <= 10000; i++ {
	//		userId := (i-1)/10+1
	//		k := tree.PointerToBytes(userId)//userId
	//		v := tree.PointerToBytes(i)//主键
	//		if err := bPTreeImpl.Insert(treeHead, k, v, tree.InsertTypeAppend); err != nil {
	//			fmt.Printf("insert index %d ", i)
	//			panic(err.Error())
	//		}
	//	}
	//	bPTreeImpl.Print(treeHead,true)
	//	assert.Equal(t, treeHead.KeyNum>1000,true,"tree head key num error")
	//	list, err := bPTreeImpl.SearchByRange(treeHead, tree.PointerToBytes(1), tree.PointerToBytes(1000),1000); if err != nil {
	//		panic(err.Error())
	//	}
	//	assert.EqualValues(t, len(list),1000,"key num error")
	//	assert.EqualValues(t, list[0].Key,tree.PointerToBytes(1),"key first error")
	//	assert.EqualValues(t, list[len(list)-1].Key,tree.PointerToBytes(1000),"key last error")
	//
	//	value, err := bPTreeImpl.Search(treeHead, tree.PointerToBytes(1000)); if err != nil {
	//		panic(err.Error())
	//	}
	//	values,err := tree.ParseCollectionByte(value); if err != nil {
	//		panic(err.Error())
	//	}
	//	var ids [][]byte
	//	for i := tree.Pointer(9991); i <= 10000; i++ {
	//		ids = append(ids, tree.PointerToBytes(i))
	//	}
	//	assert.Equal(t, values, ids,"search failed")
	//}
}

//func find(){
//	cache,err := createTreeNodeCache(treeHead,false); if err != nil {
//		panic(err.Error())
//	}
//	root,err := bPTreeImpl.getNodePosition(treeHead.Root,nil,0, cache); if err != nil {
//		panic(err.Error())
//	}
//	key := tree.PointerToBytes(899)
//	fmt.Println(key)
//	d,err := root.binarySearch(key); if err != nil {
//		panic(err.Error())
//	}
//	fmt.Println(d.Data.Key)
//	fmt.Println(d.KeyPosition.Compare)
//	node6,err := bPTreeImpl.getNodePosition(tree.Pointer(6),nil,0, cache); if err != nil {
//		panic(err.Error())
//	}
//	c,err := node6.binarySearch(key); if err != nil {
//		panic(err.Error())
//	}
//	fmt.Println(c.Data.Key)
//	fmt.Println(c.KeyPosition.Compare)
//	fmt.Println(node6.Node.Keys)
//
//	keys := node6.Node.Keys
//	front := int16(0)
//	end := int16(len(keys)) - 1
//	for front <= end {
//		mid := (front + end) / 2
//		current := keys[mid]
//		fmt.Println(current)
//		if bytes.Compare(key, current) == 0 {
//			//nodePosition.createTreeKeyData(current, values[mid], mid, tree.CompareEq)
//			fmt.Println(tree.CompareEq)
//			break
//		} else if bytes.Compare(key, current) == 1 {
//			next := keys[mid+1]
//			if bytes.Compare(key, next) == -1 {
//				//nodePosition.createTreeKeyData(current, values[mid], mid, tree.CompareGt)
//				fmt.Println(tree.CompareGt)
//				break
//			}
//			front = mid + 1
//		} else {
//			prevIndex := mid - 1
//			prev := keys[prevIndex]
//			if bytes.Compare(key, prev) == 1 {
//				//nodePosition.createTreeKeyData(prev, values[prevIndex], prevIndex, tree.CompareGt)
//				fmt.Println(tree.CompareGt)
//				break
//			}
//			end = mid - 1
//		}
//	}
//	fmt.Println("none")
//}