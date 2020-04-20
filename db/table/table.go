package table

import (
	"encoding/json"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
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