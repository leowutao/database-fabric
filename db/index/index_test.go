package index

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/protos"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
	"gitee.com/bidpoc/database-fabric-cc/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrimaryKey(t *testing.T) {
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	indexService := NewIndexService(state)
	database := db.DatabaseID(1)
	tableData := &db.TableData{Id:db.TableID(1),Name:"TestTable",Columns:nil,PrimaryKey:db.PrimaryKey{ColumnID:db.ColumnID(1),AutoIncrement:true},ForeignKeys:nil}
	//主键验证
	{
		start := db.RowID(1)
		size := db.RowID(1000)
		for i:=start; i <= size; i++ {
			blockID := db.BlockID(i/10)
			if i%10 > 0 {
				blockID++
			}
			if err := indexService.PutPrimaryKeyIndex(database, tableData, i, db.ADD, blockID); err != nil {
				fmt.Println(i)
				panic(err.Error())
			}
		}

		//打印索引树
		//treeHead,err := indexService.getTreeHead(db.ColumnKey{Database:database,Table:tableData.Id,Column:tableData.PrimaryKey.ColumnID})
		//err = indexService.iTree.Print(treeHead,true); if err != nil {
		//	panic(err.Error())
		//}

		//全表扫描
		rowBlockIDList,err := indexService.GetPrimaryKeyIndexByRange(database, tableData, start, size, db.ASC, int32(size)); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, len(rowBlockIDList), size,"row len error")
		assert.EqualValues(t, rowBlockIDList[0].RowID, start,"row start error")
		assert.EqualValues(t, rowBlockIDList[0].BlockID, start,"block start error")
		assert.EqualValues(t, rowBlockIDList[len(rowBlockIDList)-1].RowID, size,"row end error")
		assert.EqualValues(t, rowBlockIDList[len(rowBlockIDList)-1].BlockID, size/10,"block end error")
		//精确查找
		findRowID := db.RowID(100)
		findBlockID := findRowID/10
		if findRowID%10 > 0 {
			findBlockID++
		}
		blockID,err := indexService.GetPrimaryKeyIndex(database, tableData, findRowID); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, blockID, findBlockID,"block error")
		//主键唯一性验证-未删除
		err = indexService.PutPrimaryKeyIndex(database, tableData, start, db.ADD, db.BlockID(1))
		assert.NotNil(t, err,"primary already error")
		assert.EqualValues(t, err.Error(), fmt.Sprintf("primary key `%v` is already", util.RowIDToBytes(start)),"primary already error")
		//主键唯一性验证-已删除
		err = indexService.PutPrimaryKeyIndex(database, tableData, start, db.DELETE, db.BlockID(1)); if err != nil {
			panic(err.Error())
		}
		err = indexService.PutPrimaryKeyIndex(database, tableData, start, db.ADD, db.BlockID(1))
		assert.Nil(t, err,"add primary error")
		//历史插入(由于原记录集合含有新增，以下记录集合数量需要加原纪录数量)
		_,count,err := indexService.GetPrimaryKeyIndexHistoryByRange(database, tableData, start, db.DESC,100); if err != nil {
			panic(err.Error())
		}
		historyStart := db.BlockID(1)
		historySize := db.BlockID(1000)
		for i:=historyStart;i<=historySize;i++ {
			op := db.UPDATE
			if i%10 == 0 {
				op = db.DELETE
			}
			err = indexService.PutPrimaryKeyIndex(database, tableData, start, op, i); if err != nil {
				panic(err.Error())
			}
		}

		//打印链表记录
		//linkedHead,err := indexService.getLinkedHead(db.ColumnKey{Database:database,Table:tableData.Id,Column:tableData.PrimaryKey.ColumnID}, start)
		//err = indexService.iLinked.Print(linkedHead); if err != nil {
		//	panic(err.Error())
		//}

		//历史记录查询-降序
		blockIDList,total,err := indexService.GetPrimaryKeyIndexHistoryByRange(database, tableData, start, db.DESC, historySize+int32(count)); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, len(blockIDList), total,"history len error")
		assert.EqualValues(t, len(blockIDList), historySize+int32(count),"history len error")
		assert.EqualValues(t, blockIDList[0], historySize,"history start error")
		assert.EqualValues(t, blockIDList[len(blockIDList)-int(count)], historyStart,"history end error")
		//历史记录查询-升序
		blockIDList,total,err = indexService.GetPrimaryKeyIndexHistoryByRange(database, tableData, start, db.ASC, historySize+int32(count)); if err != nil {
			panic(err.Error())
		}
		assert.EqualValues(t, len(blockIDList), total,"history len error")
		assert.EqualValues(t, len(blockIDList), historySize+int32(count),"history len error")
		assert.EqualValues(t, blockIDList[count], historyStart,"history start error")
		assert.EqualValues(t, blockIDList[len(blockIDList)-1], historySize,"history end error")
	}
}

func TestForeignKey(t *testing.T) {
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	indexService := NewIndexService(state)
	database := db.DatabaseID(1)
	tableData := &db.TableData{Id:db.TableID(1),Name:"TestTable",
		Columns:[]db.Column{{}},
		PrimaryKey:db.PrimaryKey{ColumnID:db.ColumnID(1),AutoIncrement:true},
		ForeignKeys:[]db.ForeignKey{{ColumnID:db.ColumnID(2),Reference:db.ReferenceKey{TableID:db.TableID(2),ColumnID:db.ColumnID(1)}}}}
	//外键验证
	{
		start := db.RowID(1)
		size := db.RowID(1000)
		referenceRows := make(map[db.RowID][]db.RowID, size/10)
		for i := start; i <= size; i++ {
			referenceRowID := db.RowID(i/10)
			if i%10 > 0 {
				referenceRowID++
			}
			row := &protos.RowData{Columns: []*protos.ColumnData{{Data: util.RowIDToBytes(i)}, {Data: util.RowIDToBytes(referenceRowID)}}}
			if err := indexService.PutForeignKeysIndex(database, tableData, i, row); err != nil {
				fmt.Println(i)
				panic(err.Error())
			}
			rowIDs := referenceRows[referenceRowID]
			referenceRows[referenceRowID] = append(rowIDs, i)
		}

		//打印索引树
		//treeHead,err := indexService.getTreeHead(db.ColumnKey{Database:database,Table:tableData.Id,Column:tableData.PrimaryKey.ColumnID})
		//err = indexService.iTree.Print(treeHead,true); if err != nil {
		//	panic(err.Error())
		//}

		//根据外键索引查找主键
		for referenceRowID,rowIDs := range referenceRows {
			size := int32(len(rowIDs))
			rowIDList, err := indexService.GetForeignKeyIndex(database, tableData.Id, tableData.ForeignKeys[0], referenceRowID, size);if err != nil {
				panic(err.Error())
			}
			assert.EqualValues(t, len(rowIDList), size, fmt.Sprintf("reference row `%d` len error", referenceRowID))
			for i,rowID := range rowIDs {
				assert.EqualValues(t, rowID, rowIDList[i], fmt.Sprintf("reference row `%d` rowIDs error", referenceRowID))
			}
		}
	}
}