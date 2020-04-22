package table

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
)

func ValidateNull(tableName string, iDatabase db.DatabaseInterface) error {
	tableID,err := ValidateNullOfID(tableName, iDatabase); if err != nil {
		return err
	}
	if tableID == 0 {
		return fmt.Errorf("table `%s` not exists", tableName)
	}
	return nil
}

func ValidateExists(tableName string, iDatabase db.DatabaseInterface) error {
	if tableName == "" {
		return fmt.Errorf("tableName is null")
	}
	tableID,err := iDatabase.GetTableID(tableName); if err != nil {
		return err
	}
	if tableID > 0 {
		return fmt.Errorf("table `%s` already exists", tableName)
	}
	return nil
}

func ValidateNullOfID(tableName string, iDatabase db.DatabaseInterface) (db.TableID,error) {
	if tableName == "" {
		return 0,fmt.Errorf("tableName is null")
	}
	tableID,err := iDatabase.GetTableID(tableName); if err != nil {
		return tableID,err
	}
	if tableID == 0 {
		return tableID,fmt.Errorf("table `%s` not exists", tableName)
	}
	return tableID,nil
}

func ValidateNullOfData(tableName string, iDatabase db.DatabaseInterface) (*db.Table,error) {
	if tableName == "" {
		return nil,fmt.Errorf("tableName is null")
	}
	data,err := iDatabase.QueryTableDataByName(tableName); if err != nil {
		return nil,err
	}
	if data.Columns == nil && len(data.Columns) == 0 {
		return nil,fmt.Errorf("table `%s` is null", tableName)
	}
	primary := data.Columns[data.PrimaryKey.ColumnID-1]
	foreignKeys := db.ForeignKeys{}
	for _,foreignKey := range data.ForeignKeys {
		foreignKeys[foreignKey.ColumnID] = &foreignKey
	}
	return &db.Table{Data:data,Primary:&primary,ForeignKeys:foreignKeys},nil
}