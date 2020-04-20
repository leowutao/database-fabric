package history

import (
	"encoding/json"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type HistoryService struct {
	storage *storage.HistoryStorage
}

func NewHistoryService(state state.ChainCodeState) *HistoryService {
	return &HistoryService{storage.NewHistoryStorage(state)}
}

type HistoryData struct {
	Op        state.Op    `json:"op"`
	TxID      string      `json:"txID"`
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

////////////////// Public Function //////////////////
func (service *HistoryService) QueryTableHistoryBytes(tableName string, pageSize int32) ([]byte,error)  {
	pagination,err := service.queryTableHistory(tableName, pageSize)
	if err != nil {
		return nil,err
	}
	return util.ConvertJsonBytes(pagination)
}

func (service *HistoryService) QuerySchemaHistoryBytes(schemaName string, pageSize int32) ([]byte,error)  {
	pagination,err := service.querySchemaHistory(schemaName, pageSize)
	if err != nil {
		return nil,err
	}
	return util.ConvertJsonBytes(pagination)
}

func (service *HistoryService) QueryRowHistoryBytes(tableName string, id string, pageSize int32) ([]byte,error)  {
	pagination,err := service.queryRowHistory(tableName, id, pageSize)
	if err != nil {
		return nil,err
	}
	return util.ConvertJsonBytes(pagination)
}

////////////////// Private Function //////////////////
func (service *HistoryService) getHistoryList(historyBytes [][]byte) ([]state.History,error) {
	var values []state.History
	if len(historyBytes) > 0 {
		for _,b := range historyBytes {
			if len(b) > 0 {
				var history state.History
				err := json.Unmarshal(b, &history); if err != nil {
					return nil,err
				}
				values = append(values, history)
			}
		}
	}
	return values,nil
}

func (service *HistoryService) queryTableHistory(tableName string, pageSize int32) (db.Pagination,error) {
	pagination := db.Pagination{}
	var values []interface{}
	historyBytes,err := service.storage.GetTableDataHistory(tableName, pageSize)
	if err != nil {
		return pagination,err
	}
	historyList,err :=service.getHistoryList(historyBytes); if err != nil {
		return pagination,err
	}
	for _,v := range historyList {
		var table interface{}
		if len(v.Value) > 0 {
			table = db.Table{}
			err = json.Unmarshal(v.Value, &table)
			if err != nil {
				return pagination, err
			}
		}
		values = append(values, HistoryData{v.Op,v.TxID,v.Timestamp,table})
	}

	return util.Pagination(pageSize, service.storage.GetTableDataHistoryTotal(tableName), values),nil
}

func (service *HistoryService) querySchemaHistory(schemaName string, pageSize int32) (db.Pagination,error) {
	pagination := db.Pagination{}
	var values []interface{}
	historyBytes,err := service.storage.GetSchemaDataHistory(schemaName, pageSize)
	if err != nil {
		return pagination,err
	}
	historyList,err :=service.getHistoryList(historyBytes); if err != nil {
		return pagination,err
	}
	for _,v := range historyList {
		var schema interface{}
		if len(v.Value) > 0 {
			schema = db.Schema{}
			err = json.Unmarshal(v.Value, &schema)
			if err != nil {
				return pagination,err
			}
		}
		values = append(values, HistoryData{v.Op,v.TxID,v.Timestamp,schema})
	}

	return util.Pagination(pageSize, service.storage.GetSchemaDataHistoryTotal(schemaName), values),nil
}

func (service *HistoryService) queryRowHistory(tableName string, id string, pageSize int32) (db.Pagination,error) {
	pagination := db.Pagination{}
	var values []interface{}
	historyBytes,err := service.storage.GetRowDataHistory(tableName, id, pageSize)
	if err != nil {
		return pagination,err
	}
	historyList,err :=service.getHistoryList(historyBytes); if err != nil {
		return pagination,err
	}
	for _,v := range historyList {
		var row map[string]interface{}
		if len(v.Value) > 0 {
			err = json.Unmarshal(v.Value, &row)
			if err != nil {
				return pagination,err
			}
		}
		values = append(values, HistoryData{v.Op,v.TxID,v.Timestamp,row})
	}

	return util.Pagination(pageSize, service.storage.GetRowDataHistoryTotal(tableName, id), values),nil
}