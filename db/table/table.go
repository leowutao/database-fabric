package table

import (
	"bytes"
	"encoding/json"
	"github.com/bidpoc/database-fabric-cc/db"
	"github.com/bidpoc/database-fabric-cc/db/storage"
	"github.com/bidpoc/database-fabric-cc/db/storage/state"
	"github.com/bidpoc/database-fabric-cc/db/util"
)

type TableService struct {
	database *db.DataBase
	storage *storage.TableStorage
}

func NewTableService(database *db.DataBase, state state.ChainCodeState) *TableService {
	return &TableService{database,storage.NewTableStorage(state)}
}

func (service *TableService) QueryTable(tableID db.TableID) (*db.TableData,error) {
	table := &db.TableData{}
	tableBytes,err := service.storage.GetTableData(service.database.Id, tableID)
	if err != nil {
		return nil,err
	}
	if len(tableBytes) > 0 {
		err = json.Unmarshal(tableBytes, table)
		if err != nil {
			return nil,err
		}
	}
	return table,nil
}

func (service *TableService) PutTableData(table *db.TableData) error {
	bytes,err := util.ConvertJsonBytes(*table); if err != nil {
		return err
	}
	return service.storage.PutTableData(service.database.Id, table.Id, bytes)
}

func (service *TableService) CompareTable(table1 *db.TableData, table2 *db.TableData) string {
	if table1.Id != table2.Id {
		return "Id error"
	}else if table1.Name != table2.Name {
		return "Name error"
	}else if len(table1.Columns) != len(table2.Columns) {
		return "Columns len error"
	}
	for i:=0;i< len(table1.Columns);i++ {
		if table1.Columns[i].Id != table2.Columns[i].Id {
			return "Column id error"
		}else if table1.Columns[i].Name != table2.Columns[i].Name {
			return "Column Name error"
		}else if table1.Columns[i].Type != table2.Columns[i].Type {
			return "Column Type error"
		}else if !bytes.Equal(table1.Columns[i].Default, table2.Columns[i].Default)  {
			return "Column Default error"
		}else if table1.Columns[i].NotNull != table2.Columns[i].NotNull {
			return "Column NotNull error"
		}else if table1.Columns[i].Desc != table2.Columns[i].Desc {
			return "Column Desc error"
		}else if table1.Columns[i].IsDeleted != table2.Columns[i].IsDeleted {
			return "Column IsDeleted error"
		}else if table1.Columns[i].Order != table2.Columns[i].Order {
			return "Column Order error"
		}
	}
	if table1.PrimaryKey.ColumnID != table2.PrimaryKey.ColumnID {
		return "PrimaryKey ColumnID error"
	}else if table1.PrimaryKey.AutoIncrement != table2.PrimaryKey.AutoIncrement {
		return "PrimaryKey AutoIncrement error"
	}else if len(table1.ForeignKeys) != len(table2.ForeignKeys) {
		return "ForeignKeys len error"
	}
	for i:=0;i< len(table1.ForeignKeys);i++ {
		if table1.ForeignKeys[i].ColumnID != table2.ForeignKeys[i].ColumnID {
			return "ForeignKey ColumnID error"
		}else if table1.ForeignKeys[i].Reference.TableID != table2.ForeignKeys[i].Reference.TableID {
			return "ForeignKey Reference TableID error"
		}else if table1.ForeignKeys[i].Reference.ColumnID != table2.ForeignKeys[i].Reference.ColumnID {
			return "ForeignKey Reference ColumnID error"
		}
	}
	return ""
}