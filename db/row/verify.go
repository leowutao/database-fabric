package row

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index"
	"gitee.com/bidpoc/database-fabric-cc/db/table"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

func (service *RowService) verifyForeignKeys(foreignKeys []db.ForeignKey, column db.Column, value interface{}) error {
	match,foreignKey := util.MatchForeignKeyByKey(foreignKeys, column.Name)
	if value != nil && match {
		idValue,_ := util.ConvertString(value)
		if err := service.validateRowIsNotNull(foreignKey.Reference.Table, idValue); err != nil {
			return fmt.Errorf("foreignKey `%s` data `%s` not exists in table `%s`", column.Name, idValue, foreignKey.Reference.Table)
		}
	}
	return nil
}

func (service *RowService) verifyReferenceByDelRow(table db.Table, id string, indexService *index.IndexService) error {
	foreignKey,err := indexService.GetForeignKeyByReference(table.Name, table.PrimaryKey.Column); if err != nil {
		return err
	}

	if foreignKey.ForeignKey.Table != "" {
		idValue,err := indexService.QueryRowIdByIndex(foreignKey.ForeignKey.Table, foreignKey.ForeignKey.Column, id); if err != nil {
			return err
		}
		if idValue != "" {
			return fmt.Errorf("table `%s` column `%s` reference table `%s` column `%s` data `%s` ", foreignKey.ForeignKey.Table, foreignKey.ForeignKey.Column,
				foreignKey.Reference.Table, foreignKey.Reference.Column, id)
		}
	}
	return nil
}

func (service *RowService) validateRowExists(tableName string, id string) ([]byte,error) {
	bytes,err := service.storage.GetRowDataByFilter(tableName, id,true)
	if err != nil {
		return nil,err
	}
	if len(bytes) == 0 {
		return nil,fmt.Errorf("row `%s` not exists in table `%s`", id, tableName)
	}
	return bytes,nil
}

func (service *RowService) validateRowNotExists(tableName string, id string) error {
	bytes,err := service.storage.GetRowDataByFilter(tableName, id,true)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("row `%s` already exists in table `%s`", id, tableName)
	}
	return nil
}

func (service *RowService) validateRowIsNotNull(tableName string, id string) error {
	bytes,err := service.storage.GetRowData(tableName, id)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return fmt.Errorf("row `%s` not exists in table `%s`", id, tableName)
	}
	return nil
}

func (service *RowService) validateRowIsNull(tableName string, id string) error {
	bytes,err := service.storage.GetRowData(tableName, id)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("row `%s` already exists in table `%s`", id, tableName)
	}
	return nil
}

func (service *RowService) VerifyRow(table db.Table, id string, data map[string]interface{}, op db.OpType, tableService *table.TableService) (string,string,map[string]interface{},error) {
	idKey := ""
	idValue := ""
	tableName := table.Name
	newData := map[string]interface{}{}
	if op == db.UPDATE {
		version,err := service.validateRowExists(tableName, id); if err != nil {
			return idKey,idValue,newData,err
		}
		oldData,err := service.queryRowByVersion(tableName, id, version); if err != nil {
			return idKey,idValue,newData,err
		}

		if oldData == nil {
			return idKey,idValue,newData,fmt.Errorf("not find rowId `%s` in table`%s` ", id, tableName)
		}
		for k,v := range data {
			oldData[k] = v
		}
		data = oldData
		idKey = table.PrimaryKey.Column
		idValue = id
	}else if op == db.ADD {
		for _,column := range table.Columns {
			if table.PrimaryKey.AutoIncrement && column.Name == table.PrimaryKey.Column {
				continue
			}
			_,ok := data[column.Name]
			if !ok {
				return idKey,idValue,newData,fmt.Errorf("`%s` not exists in table `%s`", column.Name, tableName)
			}
		}
	}

	for k,v := range data {
		column,err := util.VerifyColumn(table.Columns, k, table.Name, k); if err != nil {
			return idKey,idValue,newData,err
		}

		value,err := util.VerifyColumnData(table.PrimaryKey, column, v); if err != nil {
			return idKey,idValue,newData,err
		}

		newData[k] = value

		if len(table.ForeignKeys) > 0 {
			err = service.verifyForeignKeys(table.ForeignKeys, column, value); if err != nil {
				return idKey,idValue,newData,err
			}
		}
	}

	if op == db.ADD {
		idKey,idValue = util.GetTablePrimaryKey(table, newData)
		err := service.validateRowNotExists(tableName, idValue); if err != nil {
			return idKey,idValue,newData,err
		}
		if table.PrimaryKey.AutoIncrement {
			var rowId int64 = 0
			if newData[idKey] != nil {
				rowId = newData[idKey].(int64)
			}
			if rowId > 0 {
				idValue = util.Int64ToString(rowId)
			}else{
				autoId,err := service.autoIncrement(tableName, tableService)
				if err != nil {
					return idKey,idValue,newData,err
				}
				newData[idKey] = autoId
				idValue = util.Int64ToString(autoId)
			}
		}
	}

	return idKey,idValue,newData,nil
}