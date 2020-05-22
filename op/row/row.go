package row

import (
	"encoding/json"
	"fmt"
	"github.com/bidpoc/database-fabric-cc/db"
	"github.com/bidpoc/database-fabric-cc/db/util"
	"github.com/bidpoc/database-fabric-cc/op/table"
	"github.com/bidpoc/database-fabric-cc/protos/db/row"
)

type RowOperation struct {
	iDatabase db.DatabaseInterface
}

func NewRowOperation(iDatabase db.DatabaseInterface) *RowOperation {
	return &RowOperation{iDatabase}
}

////////////////// Public Function //////////////////
func (operation *RowOperation) Add(tableName string, jsonString string) ([]db.RowID,error) {
	return operation.AddOrUpdate(tableName, jsonString, db.ADD)
}

func (operation *RowOperation) Update(tableName string, jsonString string) ([]db.RowID,error) {
	return operation.AddOrUpdate(tableName, jsonString, db.UPDATE)
}

func (operation *RowOperation) Delete(tableName string, rowIDs []db.RowID) ([]db.RowID,error) {
	table,err := table.ValidateNullOfData(tableName, operation.iDatabase); if err != nil {
		return nil,err
	}
	rowJsonArray := make([]db.JsonData, 0, len(rowIDs))
	for _,rowID := range rowIDs {
		rowJsonArray = append(rowJsonArray, db.JsonData{table.Primary.Name:rowID})
	}
	return operation.SetRow(table, rowJsonArray, db.DELETE)
}

func (operation *RowOperation) QueryRowBytes(tableName string, rowID db.RowID) ([]byte,error) {
	table,err := table.ValidateNullOfData(tableName, operation.iDatabase); if err != nil {
		return nil,err
	}
	jsonData,err := operation.QueryRow(table, rowID); if err != nil {
		return nil,err
	}
	return util.ConvertJsonBytes(jsonData)
}

func (operation *RowOperation) QueryRowWithPaginationBytes(tableName string, start db.RowID, end db.RowID, order db.OrderType, pageSize int32) ([]byte,error) {
	table,err := table.ValidateNullOfData(tableName, operation.iDatabase); if err != nil {
		return nil,err
	}
	pagination,err := operation.QueryRowWithPagination(table, start, end, order, pageSize); if err != nil {
		return nil,err
	}
	paginationBytes,err := util.ConvertJsonBytes(pagination); if err != nil {
		return nil,err
	}
	return paginationBytes,nil
}

func (operation *RowOperation) QueryRowDemo(tableName string) (map[string]interface{},error) {
	table,err := table.ValidateNullOfData(tableName, operation.iDatabase); if err != nil {
		return nil,err
	}
	return util.ParseRowData(table,nil)
}


////////////////// Private Function //////////////////
func (operation *RowOperation) AddOrUpdate(tableName string, jsonString string, op db.OpType) ([]db.RowID,error) {
	if jsonString == "" {
		return nil,fmt.Errorf("row json is null")
	}
	var rowJsonArray []db.JsonData
	if err := json.Unmarshal([]byte(jsonString), &rowJsonArray); err != nil {
		return nil,fmt.Errorf("row json  %s", err)
	}
	table,err := table.ValidateNullOfData(tableName, operation.iDatabase); if err != nil {
		return nil,err
	}
	return operation.SetRow(table, rowJsonArray, op)
}

/**
	行记录汇总
*/
func (operation *RowOperation) SetRow(table *db.Table, rowJsonArray []db.JsonData, op db.OpType) ([]db.RowID,error) {
	rowMaps := make(map[db.RowID]*row.RowData, len(rowJsonArray))
	rowIDs := make([]db.RowID, 0, len(rowJsonArray))
	newRows := make([]*row.RowData, 0, len(rowJsonArray))
	var incrementRows []*row.RowData
	if op == db.ADD {
		incrementRows = make([]*row.RowData, 0, len(rowJsonArray))
	}
	for _,rowJson := range rowJsonArray {
		rowData,err := operation.FormatRowData(table, rowJson, op); if err != nil {
			return nil,err
		}
		if rowData.Id == 0 {
			if table.Data.PrimaryKey.AutoIncrement {//自增行不合并
				incrementRows = append(incrementRows, rowData)
				continue
			}else{
				return nil,fmt.Errorf("SetRow rowID is null")
			}
		}
		prev,ok := rowMaps[rowData.Id]
		if ok {//存在
			prev.Columns = rowData.Columns//合并到上一次记录中
			rowData = nil//清空当前记录
		}else{
			newRows = append(newRows, rowData)
			rowMaps[rowData.Id] = rowData
			rowIDs = append(rowIDs, rowData.Id)
		}
	}
	newRows = append(newRows, incrementRows...)//自增行追加到尾端
	err := operation.iDatabase.AddRowData(table.Data, newRows); if err != nil {
		return nil,err
	}
	return rowIDs,nil
}

func (operation *RowOperation) QueryRow(table *db.Table, rowID db.RowID) (db.JsonData,error) {
	rowData,err := operation.iDatabase.QueryRowData(table.Data, rowID); if err != nil {
		return nil, err
	}
	if rowData == nil {
		return nil, nil
	}
	return util.ParseRowData(table, rowData)
}

func (operation *RowOperation) QueryRowWithPagination(table *db.Table, start db.RowID, end db.RowID, order db.OrderType, pageSize int32) (db.Pagination,error) {
	pagination := db.Pagination{}
	tally,err := operation.iDatabase.GetTableTally(table.Data.Id); if err != nil {
		return pagination,err
	}
	count := tally.AddRow - tally.DelRow
	rows,err := operation.iDatabase.QueryRowDataByRange(table.Data, start, end, order, pageSize); if err != nil {
		return pagination,err
	}
	list := make([]db.JsonData, 0, len(rows))
	for _,rowData := range rows {
		if rowData != nil && rowData.Id > 0 {
			rowJson := db.JsonData{}
			if len(rowData.Columns) > 0 {
				rowJson,err = util.ParseRowData(table, rowData); if err != nil {
					return pagination,err
				}
			}else{
				rowJson[table.Primary.Name] = rowData.Id
			}
			list = append(list, rowJson)
		}
	}
	return util.Pagination(pageSize, count, list),nil
}