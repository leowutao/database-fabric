package database

import (
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDatabase(t *testing.T) {
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	database := &db.DataBase{Id:db.DatabaseID(1)}
	databaseImpl := NewDatabaseImpl(database, state)
	tableData := &db.TableData{Name:"TestTable",
		Columns:[]db.Column{
			{Id:db.ColumnID(1),ColumnConfig:db.ColumnConfig{Name:"id",Type:db.INT,Default:nil,NotNull:true,Desc:"主键"},IsDeleted:false,Order:1},
			{Id:db.ColumnID(2),ColumnConfig:db.ColumnConfig{Name:"name",Type:db.VARCHAR,Default:nil,NotNull:false,Desc:"名字"},IsDeleted:false,Order:2},
		},
		PrimaryKey:db.PrimaryKey{ColumnID:db.ColumnID(1),AutoIncrement:true},
		ForeignKeys:[]db.ForeignKey{{ColumnID:db.ColumnID(2),Reference:db.ReferenceKey{TableID:db.TableID(2),ColumnID:db.ColumnID(1)}}}}
	tableID,err := databaseImpl.CreateTableData(tableData); if err != nil {
		panic(err.Error())
	}
	assert.EqualValues(t, tableID, db.TableID(1),"Id error")
	queryTableData,err  := databaseImpl.QueryTableDataByName(tableData.Name); if err != nil {
		panic(err.Error())
	}
	msg := databaseImpl.getTableService().CompareTable(tableData, queryTableData)
	if msg != "" {
		panic(msg)
	}
	queryTableData2,err := databaseImpl.QueryTableDataByID(db.TableID(1)); if err != nil {
		panic(err.Error())
	}
	msg2 := databaseImpl.getTableService().CompareTable(tableData, queryTableData2)
	if msg2 != "" {
		panic(msg2)
	}
	tableName,err := databaseImpl.GetTableName(db.TableID(1)); if err != nil {
		panic(err.Error())
	}
	assert.EqualValues(t, tableName, tableData.Name,"Name error")
	tableID,err = databaseImpl.GetTableID(tableData.Name); if err != nil {
		panic(err.Error())
	}
	assert.EqualValues(t, tableID, db.TableID(1),"Id error")
	//relation,err := databaseImpl.GetRelation(); if err != nil {
	//	panic(err.Error())
	//}
}