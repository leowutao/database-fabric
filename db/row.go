package db

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

type TableRow struct {
	Id string `json:"id"`
	Row map[string]interface{} `json:"row"`
}

////////////////// Public Function //////////////////
func (t *DbManager) AddRowByJson(stub shim.ChaincodeStubInterface, tableName string, rowJson string) ([]string,error) {
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

	return t.SetRow(stub, tableName, rows, ADD)
}

func (t *DbManager) UpdateRowByJson(stub shim.ChaincodeStubInterface, tableName string, rowJson string) ([]string,error) {
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

	return t.SetRow(stub, tableName, rows, UPDATE)
}

func (t *DbManager) SetRow(stub shim.ChaincodeStubInterface, tableName string, rows []TableRow, op OpType) ([]string,error) {
	var ids []string
	table,err := t.validateQueryTableIsNotNull(stub, tableName); if err != nil {
		return nil,err
	}

	for _,tableRow := range rows {
		if tableRow.Row == nil {
			return nil,fmt.Errorf("tableRow row is null")
		}
		if op == UPDATE && tableRow.Id == "" {
			return nil,fmt.Errorf("tableRow rowId is null")
		}

		idKey,idValue,row,err := t.verifyRow(stub, table, tableRow.Id, tableRow.Row, op); if err != nil {
			return nil,err
		}
		if err := t.putRow(stub, table, idKey, idValue, row, op); err != nil {
			return nil,err
		}
		ids = append(ids, idValue)
	}

	return ids,nil
}

func (t *DbManager) DelRowById(stub shim.ChaincodeStubInterface, tableName string, id string) error {
	if tableName == "" {
		return fmt.Errorf("tableName is null")
	}
	if id == "" {
		return fmt.Errorf("id is null")
	}
	table,err := t.validateQueryTableIsNotNull(stub, tableName); if err != nil {
		return err
	}

	row,err := t.queryRow(stub, tableName, id); if err != nil {
		return err
	}

	if row == nil {
		return fmt.Errorf("row is null")
	}

	return t.delRow(stub, table, id)
}

func (t *DbManager) QueryRowBytes(stub shim.ChaincodeStubInterface, tableName string, id string) ([]byte,error) {
	return t.getRowData(stub, tableName, id)
}

func (t *DbManager) QueryRowWithPaginationBytes(stub shim.ChaincodeStubInterface, tableName string, id string, pageSize int32) ([]byte,error) {
	pagination,err := t.queryRowWithPagination(stub, tableName, id, pageSize); if err != nil {
		return nil,err
	}
	paginationBytes,err := t.ConvertJsonBytes(pagination); if err != nil {
		return nil,err
	}
	return paginationBytes,nil
}

func (t *DbManager) QueryRowDemo(stub shim.ChaincodeStubInterface, tableName string) (map[string]interface{},error) {
	row := map[string]interface{}{}
	table,err := t.QueryTable(stub, tableName)
	if err != nil {
		return row,err
	}
	for _,column := range table.Columns {
		value,err := t.ConvertColumnData(column, column.Default); if err != nil {
			return row,err
		}
		row[column.Name] = value
	}
	return row,nil
}

////////////////// Private Function //////////////////

func (t *DbManager) putRow(stub shim.ChaincodeStubInterface, table Table, idKey string, idValue string, row map[string]interface{}, op OpType) error {
	id,err := t.ConvertString(row[idKey])
	if op == ADD || (op == UPDATE && id != "" && id != idValue) {
		var increment int64
		if table.PrimaryKey.AutoIncrement {
			increment = row[idKey].(int64)
		}
		if err := t.setTableTally(stub, table.Name, increment, op); err != nil {
			return err
		}
	}

	value,err := t.ConvertJsonBytes(row); if err != nil {
		return err
	}
	if err := t.putRowData(stub, table.Name, idValue, value); err != nil {
		return err
	}

	if err := t.putForeignKeyIndex(stub, table, idValue, row); err != nil {
		return err
	}
	return nil
}

func (t *DbManager) autoIncrement(stub shim.ChaincodeStubInterface, tableName string) (int64,error) {
	id,err :=  t.getTableIncrement(stub, tableName); if err != nil {
		return id,err
	}
	id = id + 1
	return id,nil
}

func (t *DbManager) queryRow(stub shim.ChaincodeStubInterface, tableName string, id string) (map[string]interface{},error) {
	row := map[string]interface{}{}
	rowBytes,err := t.getRowData(stub, tableName, id)
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

func (t *DbManager) queryRowByVersion(stub shim.ChaincodeStubInterface, tableName string, id string, version []byte) (map[string]interface{},error) {
	row := map[string]interface{}{}
	rowBytes,err := t.getRowDataByVersion(stub, tableName, id, version)
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

func (t *DbManager) queryRowWithPagination(stub shim.ChaincodeStubInterface, tableName string, id string, pageSize int32) (Pagination,error) {
	pagination := Pagination{}
	var rows []interface{}
	rowsBytes,err := t.getRowDataByRange(stub, tableName, id, pageSize); if err != nil {
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
	count,err := t.getTableCount(stub, tableName); if err != nil {
		return pagination,err
	}
	return t.Pagination(pageSize, count, rows),nil
}

func (t *DbManager) delRowByObj(stub shim.ChaincodeStubInterface, tableName string, row map[string]interface{}) error {
	table,err := t.validateQueryTableIsNotNull(stub, tableName); if err != nil {
		return err
	}
	_,id := t.getTablePrimaryKey(table, row)
	return t.delRow(stub, table, id)
}

func (t *DbManager) delRow(stub shim.ChaincodeStubInterface, table Table, id string) error {
	err := t.verifyReferenceByDelRow(stub, table, id); if err != nil {
		return err
	}

	//err = t.DelForeignKeyIndex(stub, table, row); if err != nil {
	//	return err
	//}

	if err := t.setTableTally(stub, table.Name,0, DELETE); err != nil {
		return err
	}

	return t.delRowData(stub, table.Name, id)
}