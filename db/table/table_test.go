package table

import (
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/test"
	"testing"
)

func TestTable(t *testing.T) {
	var stub = new(test.TestChaincodeStub)
	state := state.NewStateImpl(stub)
	database := &db.DataBase{Id:db.DatabaseID(1)}
	tableService := NewTableService(database, state)
	tableData := &db.TableData{Id:db.TableID(1),Name:"TestTable",
		Columns:[]db.Column{
			{Id:db.ColumnID(1),ColumnConfig:db.ColumnConfig{Name:"id",Type:db.INT,Default:nil,NotNull:true,Desc:"主键"},IsDeleted:false,Order:1},
			{Id:db.ColumnID(2),ColumnConfig:db.ColumnConfig{Name:"name",Type:db.VARCHAR,Default:nil,NotNull:false,Desc:"名字"},IsDeleted:false,Order:2},
		},
		PrimaryKey:db.PrimaryKey{ColumnID:db.ColumnID(1),AutoIncrement:true},
		ForeignKeys:[]db.ForeignKey{{ColumnID:db.ColumnID(2),Reference:db.ReferenceKey{TableID:db.TableID(2),ColumnID:db.ColumnID(1)}}}}
	err := tableService.PutTableData(tableData); if err != nil {
		panic(err.Error())
	}
	queryTableData,err  := tableService.QueryTable(db.TableID(1)); if err != nil {
		panic(err.Error())
	}
	msg := tableService.CompareTable(tableData, queryTableData)
	if msg != "" {
		panic(msg)
	}
}