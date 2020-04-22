package database

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/block"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/table"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type DatabaseImpl struct {
	database *db.DataBase
	state state.ChainCodeState
	storage *storage.DatabaseStorage
	tableService *table.TableService
	blockService *block.BlockService
}

func NewDatabaseImpl(database *db.DataBase, state state.ChainCodeState) *DatabaseImpl {
	return &DatabaseImpl{database,state,storage.NewDatabaseStorage(state),nil,nil}
}

func (service *DatabaseImpl) getTableService() *table.TableService {
	if service.tableService == nil {
		service.tableService = table.NewTableService(service.database, service.state)
	}
	return service.tableService
}

func (service *DatabaseImpl) getBlockService() *block.BlockService {
	if service.blockService == nil {
		service.blockService = block.NewBlockService(service.database, service.state)
	}
	return service.blockService
}

////////////////////////// impl database interface //////////////////////////


func (service *DatabaseImpl) GetRelation() (*db.Relation,error) {
	relation := &db.Relation{}
	relationBytes,err := service.storage.GetRelationData(service.database.Id)
	if err != nil {
		return nil,err
	}
	if len(relationBytes) > 0 {
		err = json.Unmarshal(relationBytes, relation)
		if err != nil {
			return nil,err
		}
	}
	return relation,nil
}

func (service *DatabaseImpl) GetRelationKeysByReference(reference db.ReferenceKey) ([]db.RelationKey,error) {
	return GetRelationKeysByReference(reference, service.database.Relation)
}

func (service *DatabaseImpl) GetTableTally(tableID db.TableID) (*db.TableTally,error) {
	value,err := service.storage.GetTableTally(service.database.Id, tableID); if err != nil {
		return nil,err
	}
	tally := &db.TableTally{}
	if len(value) > 0 {
		err = json.Unmarshal(value, tally)
		if err != nil {
			return nil,err
		}
	}
	return tally,nil
}

func (service *DatabaseImpl) GetTableName(tableID db.TableID) (string,error) {
	return service.storage.GetTableName(service.database.Id, tableID)
}

func (service *DatabaseImpl) GetTableID(name string) (db.TableID,error) {
	return service.storage.GetTable(service.database.Id, name)
}


func (service *DatabaseImpl) CreateTableData(table *db.TableData) (db.TableID,error) {
	tableID,err := service.storage.CreateTable(service.database.Id, table.Name); if err != nil {
		return tableID,err
	}
	table.Id = tableID
	return tableID,service.getTableService().PutTableData(table)
}

func (service *DatabaseImpl) UpdateTableData(table *db.TableData) error {
	name,err := service.GetTableName(table.Id); if err != nil {
		return err
	}
	if name != table.Name {
		tableID,err := service.GetTableID(table.Name); if err != nil {
			return err
		}
		if tableID > 0 {
			return fmt.Errorf("table name `%s` already exists", table.Name)
		}
		err = service.storage.UpdateTable(service.database.Id, table.Id, table.Name); if err != nil {
			return err
		}
	}
	return service.getTableService().PutTableData(table)
}

func (service *DatabaseImpl) DeleteTableData(tableID db.TableID) error {
	return service.storage.DeleteTable(service.database.Id, tableID)
}

func (service *DatabaseImpl) QueryTableDataByName(tableName string) (*db.TableData,error) {
	tableID,err := service.GetTableID(tableName); if err != nil {
		return nil,err
	}
	if tableID > 0 {
		return service.QueryTableDataByID(tableID)
	}
	return nil,nil
}

func (service *DatabaseImpl) QueryTableDataByID(tableID db.TableID) (*db.TableData,error) {
	return service.getTableService().QueryTable(tableID)
}

func (service *DatabaseImpl) AddRowData(table *db.TableData, rows []*db.RowData) error {
	tally,err := service.GetTableTally(table.Id); if err != nil {
		return err
	}
	if err := service.getBlockService().SetBlockData(table, tally, rows); err != nil {
		return err
	}
	value,err := util.ConvertJsonBytes(*tally); if err != nil {
		return err
	}
	return service.storage.PutTableTally(service.database.Id, table.Id, value)
}


func (service *DatabaseImpl) QueryRowBlockID(table *db.TableData, rowID db.RowID) (db.BlockID,error) {
	return service.getBlockService().QueryRowBlockID(table, rowID)
}

func (service *DatabaseImpl) QueryRowData(table *db.TableData, rowID db.RowID) (*db.RowData,error) {
	return service.getBlockService().QueryRowData(table, rowID)
}

func (service *DatabaseImpl) QueryRowIDByForeignKey(tableID db.TableID, foreignKey db.ForeignKey, referenceRowID db.RowID) ([]db.RowID,error) {
	return service.getBlockService().QueryRowIDByForeignKey(tableID, foreignKey, referenceRowID)
}

func (service *DatabaseImpl) QueryRowDataByRange(table *db.TableData, start db.RowID, end db.RowID, order db.OrderType, size int32) ([]*db.RowData,error) {
	return service.getBlockService().QueryRowDataByRange(table, start, end, order, size)
}

func (service *DatabaseImpl) QueryRowDataHistoryByRange(table *db.TableData, rowID db.RowID, start db.Timestamp, end db.Timestamp, order db.OrderType, size int32) ([]*db.RowDataHistory,db.Total,error) {
	return service.getBlockService().QueryRowDataHistoryByRange(table, rowID, start, end, order, size)
}
