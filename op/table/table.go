package table

import (
	"encoding/json"
	"fmt"
	"github.com/database-fabric/db"
	"github.com/database-fabric/db/util"
)

type TableOperation struct {
	iDatabase db.DatabaseInterface
}

func NewTableOperation(iDatabase db.DatabaseInterface) *TableOperation {
	return &TableOperation{iDatabase}
}

type Data struct {
	Name string `json:"name"`
	Columns []db.ColumnConfig `json:"columns"`
	PrimaryKey PrimaryKey `json:"primaryKey"`
	ForeignKeys []ForeignKey `json:"foreignKeys"`
}

type PrimaryKey struct {
	ColumnName string `json:"columnName"`
	AutoIncrement bool `json:"autoIncrement"`
}

type ForeignKey struct {
	ColumnName string `json:"columnName"`
	Reference string `json:"reference"`
}

////////////////// Public Function //////////////////
func (operation *TableOperation) Create(jsonString string) (db.TableID,error) {
	tableData,err := operation.FormatTableData(jsonString); if err != nil {
		return 0,fmt.Errorf("format table %s", err)
	}
	return operation.iDatabase.CreateTableData(tableData)
}

func (operation *TableOperation) DeleteTable(tableName string) (db.TableID,error)  {
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

func (operation *TableOperation) ParseTableData(table *db.Table) (Data,error) {
	data := Data{
		Name:table.Data.Name,
		Columns:make([]db.ColumnConfig, 0, len(table.Data.Columns)),
		PrimaryKey:PrimaryKey{ColumnName:table.Primary.Name,AutoIncrement:table.Data.PrimaryKey.AutoIncrement},
		ForeignKeys:make([]ForeignKey,0 , len(table.Data.ForeignKeys)),
	}
	columnMaps := make(map[db.ColumnID]string, len(table.Data.Columns))
	for _,column := range table.Data.Columns {
		if column.IsDeleted {
			continue
		}
		data.Columns = append(data.Columns, column.ColumnConfig)
		columnMaps[column.Id] = column.Name
	}
	for _,foreignKey := range table.Data.ForeignKeys {
		columnName,ok := columnMaps[foreignKey.ColumnID]
		if ok {
			tableName,err := operation.iDatabase.GetTableName(foreignKey.Reference.TableID); if err != nil {
				return data,err
			}
			data.ForeignKeys = append(data.ForeignKeys, ForeignKey{ColumnName:columnName,Reference:tableName})
		}
	}
	return data,nil
}

func (operation *TableOperation) FormatTableData(jsonString string) (*db.TableData,error) {
	if jsonString == "" {
		return nil,fmt.Errorf("table json is null")
	}
	var data Data
	if err := json.Unmarshal([]byte(jsonString), &data); err != nil {
		return nil,fmt.Errorf("table json %s", err)
	}
	if data.Name == "" {
		return nil,fmt.Errorf("name is null")
	}
	if len(data.Columns) == 0 {
		return nil,fmt.Errorf("columns is null")
	}
	if data.PrimaryKey.ColumnName == "" {
		return nil,fmt.Errorf("primaryKey column is null")
	}
	if err := ValidateExists(data.Name, operation.iDatabase); err != nil {
		return nil,err
	}
	tableData := &db.TableData{
		Name:data.Name,
		Columns:make([]db.Column, 0, len(data.Columns)),
		ForeignKeys:make([]db.ForeignKey, 0, len(data.ForeignKeys)),
	}
	var primary *db.Column
	columnMaps := make(map[string]*db.Column, len(data.Columns))
	for i,c := range data.Columns {
		_,ok := columnMaps[c.Name]
		if ok {
			return nil,fmt.Errorf("column `%s` is repeat", c.Name)
		}
		id := db.ColumnID(i+1)
		column := &db.Column{Id:id,ColumnConfig:c}
		if column.Name == data.PrimaryKey.ColumnName {
			primary = column
			tableData.PrimaryKey = db.PrimaryKey{ColumnID:id,AutoIncrement:data.PrimaryKey.AutoIncrement}
		}
		columnMaps[column.Name] = column
		tableData.Columns = append(tableData.Columns, *column)
	}
	if primary == nil {
		return nil,fmt.Errorf("primary `%s` not found in columns", data.PrimaryKey.ColumnName)
	}
	if primary.Type != db.INT {
		return nil,fmt.Errorf("primary `%s` type must is INT", primary.Name)
	}
	for _,key := range data.ForeignKeys {
		column,ok := columnMaps[key.ColumnName]
		if !ok {
			return nil,fmt.Errorf("foreign `%s` not found in columns", column.Name)
		}
		table,err := ValidateNullOfData(key.Reference, operation.iDatabase); if err != nil {
			return nil,fmt.Errorf("foreign error `%s`", err.Error())
		}
		if column.Type != table.Primary.Type {
			return nil,fmt.Errorf("foreign column type error")
		}
		foreignKey := db.ForeignKey{ColumnID:column.Id,Reference:db.ReferenceKey{ColumnID:table.Primary.Id,TableID:table.Data.Id}}
		tableData.ForeignKeys = append(tableData.ForeignKeys, foreignKey)
	}
	return tableData,nil
}