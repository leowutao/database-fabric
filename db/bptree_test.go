package db

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/test"
	"testing"
)

func TestBPTree(t *testing.T){
	var dbManager = new(DbManager)
	var stub = new(test.TestChaincodeStub)
	dbManager.ChainCodeStub = stub
	dbManager.CacheData = map[string][]byte{}

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
		key := dbManager.Int32ToByte(i)
		value := []byte{1}
		if err := dbManager.Insert(table, column, key, value, InsertTypeDefault); err != nil {
			fmt.Println(i)
			panic(err.Error())
		}
	}

	err := dbManager.Print(table, column); if err != nil {
		panic(err.Error())
	}

	node,err := dbManager.Search(table, column, dbManager.Int32ToByte(10)); if err != nil {
		panic(err.Error())
	}
	if node != nil {
		fmt.Println(dbManager.ConvertJsonString(*node))
	}

	return
}