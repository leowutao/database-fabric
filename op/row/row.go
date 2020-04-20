package row

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
	"gitee.com/bidpoc/database-fabric-cc/op/table"
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
	var rowJsonArray []db.JsonData
	for _,rowID := range rowIDs {
		rowJsonArray = append(rowJsonArray, db.JsonData{table.PrimaryName:rowID})
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

func (operation *RowOperation) QueryRowWithPaginationBytes(tableName string, id string, pageSize int32) ([]byte,error) {
	pagination,err := operation.QueryRowWithPagination(tableName, id, pageSize); if err != nil {
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
	return operation.ParseRowData(table,nil)
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

func (operation *RowOperation) SetRow(table *db.Table, rowJsonArray []db.JsonData, op db.OpType) ([]db.RowID,error) {
	var rows []*db.RowData
	for _,rowJson := range rowJsonArray {
		row,err := operation.FormatRowData(table, rowJson, op); if err != nil {
			return nil,err
		}
		rows = append(rows, row)
	}
	return operation.PutRow(table.Data, rows)
}

/**
	行记录汇总
 */
func (operation *RowOperation) PutRow(table *db.TableData, rows []*db.RowData) ([]db.RowID,error) {
	rowMaps := map[db.RowID]*db.RowData{}
	var rowIDs []db.RowID
	var newRows []*db.RowData
	for _,row := range rows {
		prev,ok := rowMaps[row.Id]
		if ok {//存在
			prev.Data = row.Data//合并到上一次记录中
			row = nil//清空当前记录
		}else{
			newRows = append(newRows, row)
			rowMaps[row.Id] = row
			rowIDs = append(rowIDs, row.Id)
		}
	}
	err := operation.iDatabase.AddRowData(table, newRows); if err != nil {
		return nil,err
	}

	return rowIDs,nil
}

func (operation *RowOperation) QueryRow(table *db.Table, rowID db.RowID) (db.JsonData,error) {
	rowData,err := operation.iDatabase.QueryRowData(table.Data, rowID);
	if err != nil {
		return nil, err
	}
	if rowData == nil {
		return nil, nil
	}
	return operation.ParseRowData(table, rowData)
}

func (operation *RowOperation) ParseRowData(table *db.Table, rowData *db.RowData) (db.JsonData,error) {
	var err error
	dataLength := 0
	if rowData != nil {
		dataLength = len(rowData.Data)
	}
	row := db.JsonData{}
	for i,column := range table.Data.Columns {
		if column.IsDeleted {
			continue
		}
		var data []byte
		if rowData != nil && i < dataLength {
			data = rowData.Data[i]
		}else{
			data = column.Default
		}
		var value interface{}
		if len(data) == 0 {
			value,err = util.ParseColumnDataByNull(column); if err != nil {
				return nil,err
			}
		}else {
			value,err = util.ParseColumnData(column, data); if err != nil {
				return nil,err
			}
		}
		row[column.Name] = value
	}
	return row,nil
}

func (operation *RowOperation) queryRowByVersion(tableName string, id string, version []byte) (map[string]interface{},error) {
	row := map[string]interface{}{}
	rowBytes,err := operation.storage.GetRowDataByVersion(tableName, id, version)
	if err != nil {
		return row,err
	}
	if len(rowBytes) > 0 {
		err = json.Unmarshal(rowBytes, &row)
		if err != nil {
			return row,err
		}
	}
	return row,nil
}

func (operation *RowOperation) QueryRowWithPagination(tableName string, id string, pageSize int32) (db.Pagination,error) {
	pagination := db.Pagination{}
	var rows []interface{}
	rowsBytes,err := operation.storage.GetRowDataByRange(tableName, id, pageSize); if err != nil {
		return pagination,err
	}
	if len(rowsBytes) > 0 {
		for _,rowByte := range rowsBytes {
			if len(rowByte) > 0 {
				var row map[string]interface{}
				err = json.Unmarshal(rowByte, &row)
				if err != nil {
					return pagination,err
				}
				rows = append(rows, row)
			}
		}
	}
	count,err := operation.tableoperation.GetTableCount(tableName); if err != nil {
		return pagination,err
	}
	return util.Pagination(pageSize, count, rows),nil
}

func (operation *RowOperation) DelRowByObj(tableName string, data map[string]interface{}) error {
	table,err := operation.tableoperation.ValidateQueryTableIsNotNull(tableName); if err != nil {
		return err
	}
	_,id := util.GetTablePrimaryKey(table, data)
	return operation.delRow(table, id)
}

func (operation *RowOperation) delRow(table db.Table, rowID db.RowID) error {
	err := operation.verifyReferenceByDelRow(table, id, operation.indexoperation); if err != nil {
		return err
	}
	operation.validateRowExists()
	rowData,err := operation.tableoperation.QueryRowData(table.Data, rowID); if err != nil {
		return err
	}
	if rowData == nil {
		return nil
	}
	if err := operation.tableoperation.SetTableTally(table.Name,0, db.DELETE); err != nil {
		return err
	}

	return operation.PutRow(table.Data, )
}

func (operation *RowOperation) QueryRowDataByIndex(tableName string, columnName string, columnData string) (string,map[string]interface{},error) {
	idValue := ""
	row := map[string]interface{}{}
	idValue,err := operation.indexoperation.QueryRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return idValue,row,err
	}

	row,err = operation.QueryRow(tableName, idValue); if err != nil {
		return idValue,row,err
	}

	return idValue,row,nil
}

func (operation *RowOperation) QueryRowDataListByIndex(tableName string, columnName string, columnData string) ([]string,[]map[string]interface{},error) {
	var idValues []string
	var rows []map[string]interface{}
	idValues,err := operation.indexoperation.QueryAllRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return idValues,rows,err
	}

	for _,idValue := range idValues {
		row,err := operation.QueryRow(tableName, idValue); if err != nil {
			return idValues,rows,err
		}
		rows = append(rows, row)
	}

	return idValues,rows,nil
}