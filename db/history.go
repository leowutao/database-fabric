package db

import (
	"encoding/json"
)

type HistoryData struct {
	Op Op `json:"op"`
	TxID string `json:"txID"`
	Timestamp int64 `json:"timestamp"`
	Data interface{} `json:"data"`
}

////////////////// Public Function //////////////////
func (t *DbManager) QueryTableHistoryBytes(tableName string, pageSize int32) ([]byte,error)  {
	pagination,err := t.queryTableHistory(tableName, pageSize)
	if err != nil {
		return nil,err
	}
	return t.ConvertJsonBytes(pagination)
}

func (t *DbManager) QuerySchemaHistoryBytes(schemaName string, pageSize int32) ([]byte,error)  {
	pagination,err := t.querySchemaHistory(schemaName, pageSize)
	if err != nil {
		return nil,err
	}
	return t.ConvertJsonBytes(pagination)
}

func (t *DbManager) QueryRowHistoryBytes(tableName string, id string, pageSize int32) ([]byte,error)  {
	pagination,err := t.queryRowHistory(tableName, id, pageSize)
	if err != nil {
		return nil,err
	}
	return t.ConvertJsonBytes(pagination)
}

////////////////// Private Function //////////////////
func (t *DbManager) getHistoryList(historyBytes [][]byte) ([]History,error) {
	var values []History
	if len(historyBytes) > 0 {
		for _,b := range historyBytes {
			if len(b) > 0 {
				var history History
				err := json.Unmarshal(b, &history); if err != nil {
					return nil,err
				}
				values = append(values, history)
			}
		}
	}
	return values,nil
}

func (t *DbManager) queryTableHistory(tableName string, pageSize int32) (Pagination,error) {
	pagination := Pagination{}
	var values []interface{}
	historyBytes,err := t.getTableDataHistory(tableName, pageSize)
	if err != nil {
		return pagination,err
	}
	historyList,err :=t.getHistoryList(historyBytes); if err != nil {
		return pagination,err
	}
	for _,v := range historyList {
		var table interface{}
		if len(v.Value) > 0 {
			table = Table{}
			err = json.Unmarshal(v.Value, &table)
			if err != nil {
				return pagination, err
			}
		}
		values = append(values, HistoryData{v.Op,v.TxID,v.Timestamp,table})
	}

	return t.Pagination(pageSize, t.getTableDataHistoryTotal(tableName), values),nil
}

func (t *DbManager) querySchemaHistory(schemaName string, pageSize int32) (Pagination,error) {
	pagination := Pagination{}
	var values []interface{}
	historyBytes,err := t.getSchemaDataHistory(schemaName, pageSize)
	if err != nil {
		return pagination,err
	}
	historyList,err :=t.getHistoryList(historyBytes); if err != nil {
		return pagination,err
	}
	for _,v := range historyList {
		var schema interface{}
		if len(v.Value) > 0 {
			schema = Schema{}
			err = json.Unmarshal(v.Value, &schema)
			if err != nil {
				return pagination,err
			}
		}
		values = append(values, HistoryData{v.Op,v.TxID,v.Timestamp,schema})
	}

	return t.Pagination(pageSize, t.getSchemaDataHistoryTotal(schemaName), values),nil
}

func (t *DbManager) queryRowHistory(tableName string, id string, pageSize int32) (Pagination,error) {
	pagination := Pagination{}
	var values []interface{}
	historyBytes,err := t.getRowDataHistory(tableName, id, pageSize)
	if err != nil {
		return pagination,err
	}
	historyList,err :=t.getHistoryList(historyBytes); if err != nil {
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

	return t.Pagination(pageSize, t.getRowDataHistoryTotal(tableName, id), values),nil
}