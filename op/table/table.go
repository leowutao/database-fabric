package table

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type TableOperation struct {
	iDatabase db.DatabaseInterface
}

func NewTableOperation(iDatabase db.DatabaseInterface) *TableOperation {
	return &TableOperation{iDatabase}
}

////////////////// Public Function //////////////////
func (operation *TableOperation) Create(jsonString string) (db.TableID,error) {
	tableData,err := operation.FormatTableData(jsonString, db.ADD); if err != nil {
		return 0,fmt.Errorf("format table %s", err)
	}
	return operation.iDatabase.CreateTableData(tableData)
}

func (operation *TableOperation) Update(jsonString string) (db.TableID,error) {
	tableData,err := operation.FormatTableData(jsonString, db.UPDATE); if err != nil {
		return 0,fmt.Errorf("format table %s", err)
	}
	return tableData.Id,operation.iDatabase.UpdateTableData(tableData)
}

func (operation *TableOperation) DelTable(tableName string) (db.TableID,error)  {
	if tableName == "" {
		return 0,fmt.Errorf("tableName is null")
	}
	tableData,err := operation.iDatabase.QueryTableDataByName(tableName); if err != nil {
		return 0,err
	}
	//外建约束验证
	reference := db.ReferenceKey{TableID:tableData.Id, ColumnID:tableData.PrimaryKey.ColumnID}
	relationKeys,err := operation.iDatabase.GetRelationKeysByReference(reference); if err != nil {
		return 0,nil
	}
	if len(relationKeys) > 0 {
		return 0,fmt.Errorf("table reference key must is null")
	}
	return tableData.Id,operation.iDatabase.DeleteTableData(tableData.Id)
}

func (operation *TableOperation) QueryTableData(tableName string) ([]byte,error) {
	tableData,err := operation.iDatabase.QueryTableDataByName(tableName); if err != nil {
		return nil,err
	}
	return util.ConvertJsonBytes(*tableData)
}

func (operation *TableOperation) FormatTableData(jsonString string, op db.OpType) (*db.TableData,error) {
	tableID := db.TableID(0)
	if jsonString == "" {
		return tableID,fmt.Errorf("table json is null")
	}
	var tableJson db.JsonData
	if err := json.Unmarshal([]byte(jsonString), &tableJson); err != nil {
		return tableID,fmt.Errorf("table json %s", err)
	}

	if table.Name == "" {
		return fmt.Errorf("name is null")
	}
	if err := ValidateExists(table.Name, operation.iDatabase); err != nil {
		return "",err
	}
	if table.Columns == nil || len(table.Columns) == 0 {
		return fmt.Errorf("columns key is null")
	}
	if table.PrimaryKey.ColumnId == 0 {
		return fmt.Errorf("primaryKey column is null")
	}

	if table.PrimaryKey.AutoIncrement {
		key := table.PrimaryKey.Column
		column,err := util.VerifyColumn(table.Columns, key, table.Name, key); if err != nil {
			return fmt.Errorf("primaryKey keys `%s` not found in columns", key)
		}
		if column.Type != db.INT {
			return fmt.Errorf("primaryKey autoIncrement keys `%s` type must is INT", key)
		}
	}else{
		key := table.PrimaryKey.Column
		column,err := util.VerifyColumn(table.Columns, key, table.Name, key); if err != nil {
			return fmt.Errorf("primaryKey keys `%s` not found in columns", key)
		}
		if column.Type != db.INT && column.Type != db.VARCHAR {
			return fmt.Errorf("primaryKey keys `%s` type must is INT or VARCHAR", key)
		}
	}

	if table.ForeignKeys != nil {
		for _,foreignKey := range table.ForeignKeys {
			key := foreignKey.Column
			_,err := util.VerifyColumn(table.Columns, key, table.Name, key); if err != nil {
				return fmt.Errorf("foreignKey key `%s` not found in columns", key)
			}
			relationTable,err := operation.ValidateQueryTableIsNotNull(foreignKey.Reference.Table); if err != nil {
				return err
			}
			match,relationForeignKey := util.MatchForeignKeyByTable(relationTable.ForeignKeys, table.Name); if match {
				return fmt.Errorf("table `%s` and `%s` foreignKey realtion exists", table.Name, relationForeignKey.Reference.Table)
			}
		}
	}

	var newColumns []db.Column
	for _,column := range table.Columns {
		value,err := util.ConvertColumnData(column, column.Default); if err != nil {
			return err
		}

		column.Default = value
		newColumns = append(newColumns, column)
	}

	table.Columns = newColumns
	tableByte,err := util.ConvertJsonBytes(table)
	if err != nil {
		return err
	}

	if err = operation.SetForeignKey(table, indexoperation); err != nil {
		return err
	}

	if err = operation.storage.PutTableData(operation.database, table.Id, tableByte); err != nil {
		return err
	}

	return nil
}