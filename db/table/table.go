package table

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type TableService struct {
	storage *storage.TableStorage
}

func NewTableService(state state.ChainCodeState) *TableService {
	return &TableService{storage.NewTableStorage(state)}
}

////////////////// Public Function //////////////////
func (service *TableService) AddTableByJson(tableJson string, indexService *index.IndexService) (string,error) {
	if tableJson == "" {
		return "",fmt.Errorf("tableJson is null")
	}
	var table db.Table
	if err := json.Unmarshal([]byte(tableJson), &table); err != nil {
		return "",fmt.Errorf("tableJson %s", err)
	}
	if table.Name == "" {
		return "",fmt.Errorf("name is null")
	}
	if err := service.ValidateTableNotExists(table.Name); err != nil {
		return "",err
	}
	return table.Name,service.setTable(table, indexService)
}

func (service *TableService) UpdateTableByJson(tableJson string, indexService *index.IndexService) (string,error) {
	if tableJson == "" {
		return "",fmt.Errorf("tableJson is null")
	}
	var table db.Table
	if err := json.Unmarshal([]byte(tableJson), &table); err != nil {
		return "",fmt.Errorf("tableJson %s", err)
	}
	if table.Name == "" {
		return "",fmt.Errorf("name is null")
	}
	if err := service.ValidateTableExists(table.Name); err != nil {
		return "",err
	}
	return table.Name,service.setTable(table, indexService)
}

func (service *TableService) DelTable(tableName string, indexService *index.IndexService) error {
	if tableName == "" {
		return fmt.Errorf("tableName is null")
	}
	table,err := service.ValidateQueryTableIsNotNull(tableName); if err != nil {
		return err
	}

	err = service.VerifyReferenceByDelTable(table, indexService); if err != nil {
		return err
	}

	err = service.storage.DelTableData(tableName); if err != nil {
		return err
	}

	return nil
}

func (service *TableService) QueryTableBytes(tableName string) ([]byte,error) {
	return service.storage.GetTableData(tableName)
}

func (service *TableService) QueryAllTableNameBytes() ([]byte,error) {
	tables,err := service.storage.GetAllTableKey(); if err != nil {
		return nil,err
	}
	return util.ConvertJsonBytes(tables)
}

func (service *TableService) QueryTable(tableName string) (db.Table,error) {
	table := db.Table{}
	tableBytes,err := service.storage.GetTableData(tableName)
	if err != nil {
		return table,err
	}
	if len(tableBytes) > 0 {
		err = json.Unmarshal(tableBytes, &table)
		if err != nil {
			return table,err
		}
	}
	return table,nil
}

////////////////// Private Function /////////////////
func (service *TableService) setTable(table db.Table, indexService *index.IndexService) error {
	if table.Name == "" {
		return fmt.Errorf("name is null")
	}
	if table.Columns == nil || len(table.Columns) == 0 {
		return fmt.Errorf("columns key is null")
	}
	if len(table.PrimaryKey.Column) == 0 {
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
			relationTable,err := service.ValidateQueryTableIsNotNull(foreignKey.Reference.Table); if err != nil {
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

	if err = service.SetForeignKey(table, indexService); err != nil {
		return err
	}

	if err = service.storage.PutTableData(table.Name, tableByte); err != nil {
		return err
	}

	return nil
}

func (service *TableService) SetTableTally(tableName string, increment int64, op db.OpType) error {
	tableTally,err := service.GetTableTally(tableName); if err != nil {
		return err
	}
	if op == db.ADD {
		tableTally.Count = tableTally.Count + 1
		if increment > tableTally.Increment {
			tableTally.Increment = increment
		}
	}else if op == db.UPDATE {
		if increment > tableTally.Increment {
			tableTally.Increment = increment
		}
	}else if op == db.DELETE {
		tableTally.Count = tableTally.Count - 1
	}
	value,err := util.ConvertJsonBytes(tableTally); if err != nil {
		return err
	}
	return service.storage.PutTableTallyData(tableName, value)
}

func (service *TableService) GetTableTally(tableName string) (db.TableTally,error) {
	tableTally := db.TableTally{0,0}
	value,err := service.storage.GetTableTallyData(tableName); if err != nil {
		return tableTally,err
	}
	if len(value) > 0 {
		err = json.Unmarshal(value, &tableTally); if err != nil {
			return tableTally,err
		}
	}
	return tableTally,nil
}

func (service *TableService) GetTableIncrement(tableName string) (int64,error) {
	tableTally,err := service.GetTableTally(tableName); if err != nil {
		return 0,err
	}

	return tableTally.Increment,nil
}

func (service *TableService) GetTableCount(tableName string) (int64,error) {
	tableTally,err := service.GetTableTally(tableName); if err != nil {
		return 0,err
	}
	return tableTally.Count,nil
}

func (service *TableService) VerifyReferenceByDelTable(table db.Table, indexService *index.IndexService) error {
	foreignKey,err := indexService.GetForeignKeyByReference(table.Name, table.PrimaryKey.Column); if err != nil {
		return err
	}
	if foreignKey.ForeignKey.Table != "" {
		return fmt.Errorf("table `%s` reference table `%s`", table.Name, foreignKey.ForeignKey.Table)
	}
	return nil
}

func (service *TableService) VerifyReferenceBySetTable(table db.Table) ([]db.ReferenceForeignKey,[]db.ReferenceForeignKey,error) {
	var addForeignKeys []db.ReferenceForeignKey
	var deleteForeignKeys []db.ReferenceForeignKey
	oldTable,err := service.QueryTable(table.Name); if err != nil {
		return addForeignKeys,deleteForeignKeys,err
	}
	if oldTable.Name != "" {
		for _,foreignKey := range table.ForeignKeys {
			changeType := 1 // add
			for _,oldForeignKey := range oldTable.ForeignKeys {
				if foreignKey.Column == oldForeignKey.Column {
					if foreignKey.Reference.Column == oldForeignKey.Reference.Column && foreignKey.Reference.Table == oldForeignKey.Reference.Table {
						changeType = 0 // none
					}else{
						changeType = 2 // update
					}
					break
				}
			}

			if changeType == 2 {
				deleteForeignKeys = append(addForeignKeys, db.ReferenceForeignKey{foreignKey.Reference,db.ReferenceKey{table.Name,foreignKey.Column}})
			}
			if changeType > 0 {
				addForeignKeys = append(addForeignKeys, db.ReferenceForeignKey{foreignKey.Reference,db.ReferenceKey{table.Name,foreignKey.Column}})
			}
		}

	}

	return addForeignKeys,deleteForeignKeys,nil
}

func (service *TableService) SetForeignKey(table db.Table, indexService *index.IndexService) error {
	addForeignKeys,deleteForeignKeys,err := service.VerifyReferenceBySetTable(table); if err != nil {
		return err
	}

	for _,foreignKey := range addForeignKeys {
		if err = indexService.PutForeignKey(foreignKey); err != nil {
			return err
		}
	}

	for _,foreignKey := range deleteForeignKeys {
		if err = indexService.DelForeignKey(foreignKey); err != nil {
			return err
		}
	}
	return nil
}