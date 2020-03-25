package bptree

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
	"gitee.com/bidpoc/database-fabric-cc/test"
	"testing"
)

func TestBPTree(t *testing.T){
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	bPTreeImpl := NewBPTreeImpl(storage.NewBPTreeStorage(state))

	//bd := dbManager.Int32ToByte(int32(10000))
	//key1 := []byte{}
	//key := [][]byte{}
	//for i:=0;i<200;i++ {
	//	key = append(key, bd)
	//	key1 = append(key1,111)
	//}
	//value := [][]byte{[]byte{1}}
	//b,_ := dbManager.ConvertJsonBytes(key)
	//a,_ := dbManager.ConvertJsonBytes(value)
	//fmt.Println(len(b))
	//fmt.Println(len(a))
	//
	//node := db.TreeNode{1,2,1,1,key,key}
	//n,_ := dbManager.ConvertJsonBytes(node)
	//fmt.Println(len(n))
	//
	//k,_ := dbManager.ConvertJsonBytes(key1)
	//fmt.Println(len(k))

	table := "ShoppingCart"
	column := "userId"
	for i:=int32(1);i<=10;i++ {
		key := util.Int32ToByte(i)
		value := []byte{1}
		if err := bPTreeImpl.Insert(table, column, key, value, tree.InsertTypeDefault); err != nil {
			fmt.Println(i)
			panic(err.Error())
		}
	}

	err := bPTreeImpl.Print(table, column); if err != nil {
		panic(err.Error())
	}

	value,vType,err := bPTreeImpl.Search(table, column, util.Int32ToByte(10)); if err != nil {
		panic(err.Error())
	}
	if value != nil {
		fmt.Println(vType)
		fmt.Println(util.ConvertJsonString(value))
	}

	return
}