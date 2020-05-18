package block

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
	"gitee.com/bidpoc/database-fabric-cc/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlock(t *testing.T) {
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	database := &db.DataBase{Id:db.DatabaseID(1)}
	blockService := NewBlockService(database, state)
	tableData := &db.TableData{Id:db.TableID(1),Name:"TestTable",
		Columns:[]db.Column{{}},
		PrimaryKey:db.PrimaryKey{ColumnID:db.ColumnID(1),AutoIncrement:true},
		ForeignKeys:[]db.ForeignKey{{ColumnID:db.ColumnID(2),Reference:db.ReferenceKey{TableID:db.TableID(2),ColumnID:db.ColumnID(1)}}}}
	tally := &db.TableTally{TableID:tableData.Id}
	//多行记录合并
	//{
	//	start := db.RowID(1)
	//	size := db.RowID(1000)
	//	rows := make([]*db.RowData, 0, size)
	//	referenceRows := make(map[db.RowID][]db.RowID, size/10)
	//	for i := start; i <= size; i++ {
	//		referenceRowID := db.RowID(i / 10)
	//		if i%10 > 0 {
	//			referenceRowID++
	//		}
	//		rows = append(rows, &db.RowData{Id:i,Op:db.ADD,Data:[][]byte{util.RowIDToBytes(i),util.RowIDToBytes(referenceRowID)}})
	//		rowIDs := referenceRows[referenceRowID]
	//		referenceRows[referenceRowID] = append(rowIDs, i)
	//	}
	//	if err := blockService.SetBlockData(tableData, tally, rows); err != nil {
	//		panic(err.Error())
	//	}
	//	for _, row := range rows {
	//		blockID, err := blockService.QueryRowBlockID(tableData, row.Id);
	//		if err != nil {
	//			panic(err.Error())
	//		}
	//		assert.EqualValues(t, blockID > 0,true, fmt.Sprintf("row `%d` block error", row.Id))
	//		rowData, err := blockService.QueryRowData(tableData, row.Id);
	//		if err != nil {
	//			panic(err.Error())
	//		}
	//		assert.NotNil(t, rowData, fmt.Sprintf("row `%d error", row.Id))
	//		rowHistories, total, err := blockService.QueryRowDataHistoryByRange(tableData, row.Id, db.DESC,100);
	//		if err != nil {
	//			panic(err.Error())
	//		}
	//		assert.EqualValues(t, total > 0,true, fmt.Sprintf("row `%d` history total error", row.Id))
	//		assert.EqualValues(t, len(rowHistories) > 0,true, fmt.Sprintf("row `%d` history list error", row.Id))
	//	}
	//	//行记录范围查询
	//	pageSize := int32(15)
	//	rowList, err := blockService.QueryRowDataByRange(tableData, db.RowID(0), db.RowID(0), db.ASC, pageSize);
	//	if err != nil {
	//		panic(err.Error())
	//	}
	//	assert.EqualValues(t, len(rowList), pageSize,"row len error")
	//	assert.EqualValues(t, rowList[0].Id, db.RowID(1),"row start error")
	//	assert.EqualValues(t, rowList[len(rowList)-1].Id, db.RowID(pageSize),"row end error")
	//	//根据外键查找行记录
	//	for referenceRowID,rowIDs := range referenceRows {
	//		size := int32(len(rowIDs))
	//		ids, err := blockService.QueryRowIDByForeignKey(tableData.Id, tableData.ForeignKeys[0], referenceRowID, size);if err != nil {
	//			panic(err.Error())
	//		}
	//		assert.EqualValues(t, len(ids), size,"foreign row len error")
	//		assert.EqualValues(t, ids, rowIDs,"foreign row list error")
	//	}
	//}
	//行数据分裂
	//{
	//	rowID := db.RowID(1)
	//	columnSize := useSize-rowSize
	//	columnLength := 1024
	//	columnData := make([]byte, 0, columnSize)
	//	for i:=0;i<int(columnSize);i++{
	//		columnData = append(columnData,1)
	//	}
	//	data := make([][]byte, 0, columnSize)
	//	for i:=0;i<columnLength;i++ {
	//		data = append(data, columnData)
	//	}
	//	validRowData := func(rowData *db.RowData, id db.RowID){
	//		assert.NotNil(t, rowData, fmt.Sprintf("row `%d error", id))
	//		assert.EqualValues(t, rowData.Id, id, fmt.Sprintf("row `%d error", id))
	//		assert.EqualValues(t, len(rowData.Data), columnLength,"row data len error")
	//		for i:=0;i<int(columnLength);i++{
	//			assert.EqualValues(t, len(rowData.Data[i]), len(columnData),fmt.Sprintf("row data index `%d` len error", i))
	//		}
	//	}
	//	//单行分裂
	//	{
	//		row := &db.RowData{Id:rowID,Op:db.UPDATE,Data:data}
	//		rows := []*db.RowData{row}
	//		if err := blockService.SetBlockData(tableData, tally, rows); err != nil {
	//			panic(err.Error())
	//		}
	//		blockID, err := blockService.QueryRowBlockID(tableData, rowID);
	//		if err != nil {
	//			panic(err.Error())
	//		}
	//		block,err := blockService.getBlockData(tableData.Id, blockID); if err != nil {
	//			panic(err.Error())
	//		}
	//		assert.EqualValues(t, len(block.Rows),1, "row block len error")
	//		assert.EqualValues(t, block.Join, db.JoinTypeRow,"row block join error")
	//		rowData, err := blockService.QueryRowData(tableData, rowID);
	//		if err != nil {
	//			panic(err.Error())
	//		}
	//		validRowData(rowData, rowID)
	//	}
	//	//多行分裂
	//	{
	//		size := db.RowID(3)
	//		rows := make([]*db.RowData, 0, size)
	//		for i:=rowID;i<=size;i++{
	//			rows = append(rows, &db.RowData{Id:i,Op:db.UPDATE,Data:data})
	//		}
	//		if err := blockService.SetBlockData(tableData, tally, rows); err != nil {
	//			panic(err.Error())
	//		}
	//		fmt.Println(tally.Block)
	//		rowList, err := blockService.QueryRowDataByRange(tableData, db.RowID(1), db.RowID(100), db.ASC,100);if err != nil {
	//			panic(err.Error())
	//		}
	//		assert.EqualValues(t, len(rowList), size,"row len error")
	//		for i,rowData := range rowList{
	//			validRowData(rowData, db.RowID(i+1))
	//		}
	//	}
	//}
	//列数据分裂
	{
		rowID := db.RowID(1)
		rows := make([]*db.RowData, 0, 1)
		columnSize := 1024*1024
		columnData := make([]byte, 0, columnSize)
		for i:=0;i<columnSize;i++{
			columnData = append(columnData,1)
		}
		validRowData := func(columnLength int){
			blockID, err := blockService.QueryRowBlockID(tableData, rowID);
			if err != nil {
				panic(err.Error())
			}
			block,err := blockService.getBlockData(tableData.Id, blockID); if err != nil {
				panic(err.Error())
			}
			assert.EqualValues(t, len(block.Rows),1, "row block len error")
			assert.EqualValues(t, block.Join, db.JoinTypeColumn,"row block join error")
			rowData, err := blockService.QueryRowData(tableData, rowID);
			if err != nil {
				panic(err.Error())
			}
			assert.NotNil(t, rowData, fmt.Sprintf("row `%d error", rowID))
			assert.EqualValues(t, rowData.Id, rowID, fmt.Sprintf("row `%d error", rowID))
			assert.EqualValues(t, len(rowData.Data), columnLength,"row data len error")
			assert.EqualValues(t, rowData.Data[0], util.RowIDToBytes(rowID),"row id error")
			for i:=1;i<columnLength;i++{
				assert.EqualValues(t, len(rowData.Data[i]), len(columnData),fmt.Sprintf("row data index `%d` len error", i))
			}
		}
		//单列分裂
		{
			rows = append(rows, &db.RowData{Id:rowID,Op:db.UPDATE,Data:[][]byte{util.RowIDToBytes(rowID), columnData}})
			if err := blockService.SetBlockData(tableData, tally, rows); err != nil {
				panic(err.Error())
			}
			//fmt.Println(tally.Block)
			validRowData(2)
		}
		//多列分裂
		//{
		//	columnLength := 4
		//	data := make([][]byte, 0, columnLength)
		//	data = append(data, util.RowIDToBytes(rowID))
		//	for i:=1;i<columnLength;i++ {
		//		data = append(data, columnData)
		//	}
		//	rows = append(rows, &db.RowData{Id:rowID,Op:db.UPDATE,Data:data})
		//	if err := blockService.SetBlockData(tableData, tally, rows); err != nil {
		//		panic(err.Error())
		//	}
		//	validRowData(columnLength+1)
		//}
	}
}