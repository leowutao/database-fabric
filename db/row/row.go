package row

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/table"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type RowService struct {
	storage *storage.RowStorage
}

func NewRowService(state state.ChainCodeState) *RowService {
	return &RowService{storage.NewRowStorage(state)}
}

type TableRow struct {
	Id string `json:"id"`
	Row map[string]interface{} `json:"row"`
}

////////////////// Public Function //////////////////
func (service *RowService) AddRowByJson(tableName string, rowJson string, tableService *table.TableService, indexService *index.IndexService) ([]string,error) {
	if tableName == "" {
		return nil,fmt.Errorf("tableName is null")
	}
	if rowJson == "" {
		return nil,fmt.Errorf("rowJson is null")
	}
	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(rowJson), &data); err != nil {
		return nil,fmt.Errorf("rowJson %s", err)
	}
	var rows []TableRow
	for _,row := range data {
		rows = append(rows, TableRow{"",row})
	}

	return service.SetRow(tableName, rows, db.ADD, tableService, indexService)
}

func (service *RowService) UpdateRowByJson(tableName string, rowJson string, tableService *table.TableService, indexService *index.IndexService) ([]string,error) {
	if tableName == "" {
		return nil,fmt.Errorf("tableName is null")
	}
	if rowJson == "" {
		return nil,fmt.Errorf("rowJson is null")
	}
	var rows []TableRow
	if err := json.Unmarshal([]byte(rowJson), &rows); err != nil {
		return nil,fmt.Errorf("rowJson %s", err)
	}

	for _,tableRow := range rows {
		if tableRow.Id == "" {
			return nil,fmt.Errorf("rowJson is null")
		}
	}

	return service.SetRow(tableName, rows, db.UPDATE, tableService, indexService)
}

func (service *RowService) SetRow(tableName string, rows []TableRow, op db.OpType, tableService *table.TableService, indexService *index.IndexService) ([]string,error) {
	var ids []string
	table,err := tableService.ValidateQueryTableIsNotNull(tableName); if err != nil {
		return nil,err
	}

	for _,tableRow := range rows {
		if tableRow.Row == nil {
			return nil,fmt.Errorf("tableRow row is null")
		}
		if op == db.UPDATE && tableRow.Id == "" {
			return nil,fmt.Errorf("tableRow rowId is null")
		}

		idKey,idValue,row,err := service.VerifyRow(table, tableRow.Id, tableRow.Row, op, tableService); if err != nil {
			return nil,err
		}
		if err := service.PutRow(table, idKey, idValue, row, op, tableService, indexService); err != nil {
			return nil,err
		}
		ids = append(ids, idValue)
	}

	return ids,nil
}

func (service *RowService) DelRowById(tableName string, id string, tableService *table.TableService, indexService *index.IndexService) error {
	if tableName == "" {
		return fmt.Errorf("tableName is null")
	}
	if id == "" {
		return fmt.Errorf("id is null")
	}
	table,err := tableService.ValidateQueryTableIsNotNull(tableName); if err != nil {
		return err
	}

	row,err := service.QueryRow(tableName, id); if err != nil {
		return err
	}

	if row == nil {
		return fmt.Errorf("row is null")
	}

	return service.delRow(table, id, tableService, indexService)
}

func (service *RowService) QueryRowBytes(tableName string, id string) ([]byte,error) {
	return service.storage.GetRowData(tableName, id)
}

func (service *RowService) QueryRowWithPaginationBytes(tableName string, id string, pageSize int32, tableService *table.TableService) ([]byte,error) {
	pagination,err := service.QueryRowWithPagination(tableName, id, pageSize, tableService); if err != nil {
		return nil,err
	}
	paginationBytes,err := util.ConvertJsonBytes(pagination); if err != nil {
		return nil,err
	}
	return paginationBytes,nil
}

func (service *RowService) QueryRowDemo(tableName string, tableService *table.TableService) (map[string]interface{},error) {
	row := map[string]interface{}{}
	table,err := tableService.QueryTable(tableName)
	if err != nil {
		return row,err
	}
	for _,column := range table.Columns {
		value,err := util.ConvertColumnData(column, column.Default); if err != nil {
			return row,err
		}
		row[column.Name] = value
	}
	return row,nil
}

////////////////// Private Function //////////////////

func (service *RowService) PutRow(table db.Table, idKey string, idValue string, data map[string]interface{}, op db.OpType, tableService *table.TableService, indexService *index.IndexService) error {
	id,err := util.ConvertString(data[idKey])
	if op == db.ADD || (op == db.UPDATE && id != "" && id != idValue) {
		var increment int64
		if table.PrimaryKey.AutoIncrement {
			increment = data[idKey].(int64)
		}
		if err := tableService.SetTableTally(table.Name, increment, op); err != nil {
			return err
		}
	}

	value,err := util.ConvertJsonBytes(data); if err != nil {
		return err
	}
	if err := service.storage.PutRowData(table.Name, idValue, value); err != nil {
		return err
	}

	if err := indexService.PutForeignKeyIndex(table, idValue, data); err != nil {
		return err
	}
	return nil
}

func (service *RowService) autoIncrement(tableName string, tableService *table.TableService) (int64,error) {
	id,err :=  tableService.GetTableIncrement(tableName); if err != nil {
		return id,err
	}
	id = id + 1
	return id,nil
}

func (service *RowService) QueryRow(tableName string, id string) (map[string]interface{},error) {
	row := map[string]interface{}{}
	rowBytes,err := service.storage.GetRowData(tableName, id)
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

func (service *RowService) queryRowByVersion(tableName string, id string, version []byte) (map[string]interface{},error) {
	row := map[string]interface{}{}
	rowBytes,err := service.storage.GetRowDataByVersion(tableName, id, version)
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

func (service *RowService) QueryRowWithPagination(tableName string, id string, pageSize int32, tableService *table.TableService) (db.Pagination,error) {
	pagination := db.Pagination{}
	var rows []interface{}
	rowsBytes,err := service.storage.GetRowDataByRange(tableName, id, pageSize); if err != nil {
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
	count,err := tableService.GetTableCount(tableName); if err != nil {
		return pagination,err
	}
	return util.Pagination(pageSize, count, rows),nil
}

func (service *RowService) DelRowByObj(tableName string, data map[string]interface{}, tableService *table.TableService, indexService *index.IndexService) error {
	table,err := tableService.ValidateQueryTableIsNotNull(tableName); if err != nil {
		return err
	}
	_,id := util.GetTablePrimaryKey(table, data)
	return service.delRow(table, id, tableService, indexService)
}

func (service *RowService) delRow(table db.Table, id string, tableService *table.TableService, indexService *index.IndexService) error {
	err := service.verifyReferenceByDelRow(table, id, indexService); if err != nil {
		return err
	}

	//err = t.DelForeignKeyIndex(table, row); if err != nil {
	//	return err
	//}

	if err := tableService.SetTableTally(table.Name,0, db.DELETE); err != nil {
		return err
	}

	return service.storage.DelRowData(table.Name, id)
}

func (service *RowService) QueryRowDataByIndex(tableName string, columnName string, columnData string, indexService *index.IndexService) (string,map[string]interface{},error) {
	idValue := ""
	row := map[string]interface{}{}
	idValue,err := indexService.QueryRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return idValue,row,err
	}

	row,err = service.QueryRow(tableName, idValue); if err != nil {
		return idValue,row,err
	}

	return idValue,row,nil
}

func (service *RowService) QueryRowDataListByIndex(tableName string, columnName string, columnData string, indexService *index.IndexService) ([]string,[]map[string]interface{},error) {
	var idValues []string
	var rows []map[string]interface{}
	idValues,err := indexService.QueryAllRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return idValues,rows,err
	}

	for _,idValue := range idValues {
		row,err := service.QueryRow(tableName, idValue); if err != nil {
			return idValues,rows,err
		}
		rows = append(rows, row)
	}

	return idValues,rows,nil
}