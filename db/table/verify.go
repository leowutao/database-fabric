package table

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
)

func (service *TableService) ValidateTableExists(tableName string) error {
	bytes,err := service.storage.GetTableDataByFilter(tableName,true)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return fmt.Errorf("table `%s` not exists", tableName)
	}
	return nil
}

func (service *TableService) ValidateTableNotExists(tableName string) error {
	bytes,err := service.storage.GetTableDataByFilter(tableName,true)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("table `%s` already exists", tableName)
	}
	return nil
}

func (service *TableService) ValidateQueryTableIsNotNull(tableName string) (db.Table,error) {
	data,err := service.QueryTable(tableName)
	if err != nil {
		return data,err
	}
	if data.Columns == nil && len(data.Columns) == 0 {
		return data,fmt.Errorf("table `%s` is null", tableName)
	}

	return data,nil
}