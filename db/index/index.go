package index

import (
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type IndexService struct {
	storage *storage.IndexStorage
}

func NewIndexService(state state.ChainCodeState) *IndexService {
	return &IndexService{storage.NewIndexStorage(state)}
}

func (service *IndexService) PutForeignKey(foreignKey db.ReferenceForeignKey) error {
	return service.storage.PutForeignKey(foreignKey)
}

func (service *IndexService) DelForeignKey(foreignKey db.ReferenceForeignKey) error {
	return service.storage.DelForeignKey(foreignKey)
}

func (service *IndexService) PutForeignKeyIndex(table db.Table, idValue string, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := util.ConvertString(row[column]); if err != nil {
			return err
		}
		if err := service.storage.PutRowIdIndex(table.Name, column, value, idValue); err != nil {
			return err
		}
	}

	return nil
}

func (service *IndexService) DelForeignKeyIndex(table db.Table, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := util.ConvertString(row[column]); if err != nil {
			return err
		}
		if err := service.storage.DelRowIdIndex(table.Name, column, value); err != nil {
			return err
		}
	}

	return nil
}

func (service *IndexService) QueryRowIdByIndex(tableName string, columnName string, columnData string) (string,error) {
	return service.storage.GetRowIdByIndex(tableName, columnName, columnData)
}

func (service *IndexService) QueryAllRowIdByIndex(tableName string, columnName string, columnData string) ([]string,error) {
	return service.storage.GetAllRowIdByIndex(tableName, columnName, columnData)
}

func (service *IndexService) GetForeignKeyByReference(referenceTable string, referenceColumn string) (db.ReferenceForeignKey,error) {
	return service.storage.GetForeignKeyByReference(referenceTable, referenceColumn)
}