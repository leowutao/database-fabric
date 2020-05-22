package linkedlist

import (
	"fmt"
	"github.com/database-fabric/db"
	"github.com/database-fabric/db/storage"
	"github.com/database-fabric/db/storage/state"
	"github.com/database-fabric/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLinkedList(t *testing.T) {
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	linkedListImpl := NewLinkedListImpl(storage.NewLinkedListStorage(state))
	//链表基本验证
	{
		key := db.ColumnRowKey{ColumnKey:db.ColumnKey{Database:db.DatabaseID(1),Table:db.TableID(1),Column:db.ColumnID(1)},Row:db.RowID(1)}
		linkedHead,err := linkedListImpl.CreateHead(key); if err != nil {
			panic(err.Error())
	}
		start := Pointer(1)
		size := Pointer(1000)
		for i:=start; i <= size; i++ {
			v := PointerToBytes(i)
			if err := linkedListImpl.Insert(linkedHead,[][]byte{v}); err != nil {
				fmt.Println(i)
				panic(err.Error())
			}
		}
		if err := linkedListImpl.Print(linkedHead); err != nil {
			panic(err.Error())
		}

		//头信息验证
		assert.EqualValues(t, linkedHead.Num, size,"head num error")
		assert.EqualValues(t, linkedHead.Last, linkedHead.Order,"head last error")
		//正向扫描
		list,total,err := linkedListImpl.SearchByRange(linkedHead, db.ASC, size); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, total, size,"total error")
		assert.EqualValues(t, len(list), size,"list len error")
		assert.EqualValues(t, list[0], PointerToBytes(start),"list start error")
		assert.EqualValues(t, list[len(list)-1], PointerToBytes(size),"list end error")
		//逆向扫描
		list,total,err = linkedListImpl.SearchByRange(linkedHead, db.DESC, size); if err != nil {
		panic(err.Error())
	}
		assert.EqualValues(t, total, size,"total error")
		assert.EqualValues(t, len(list), size,"list len error")
		assert.EqualValues(t, list[0], PointerToBytes(size),"list start error")
		assert.EqualValues(t, list[len(list)-1], PointerToBytes(start),"list end error")
		//指定记录数
		pageSize := Pointer(15)
		list,total,err = linkedListImpl.SearchByRange(linkedHead, db.ASC, pageSize); if err != nil {
		panic(err.Error())
	}
		assert.EqualValues(t, total, size,"total error")
		assert.EqualValues(t, len(list), pageSize,"list len error")
		assert.EqualValues(t, list[0], PointerToBytes(start),"list start error")
		assert.EqualValues(t, list[len(list)-1], PointerToBytes(pageSize),"list end error")
	}
}