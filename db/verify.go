package db

import (
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

func (t *DbManager) verifyColumn(columns []Column, k string, table string, path string) (Column,error) {
	for _, column := range columns {
		if k == column.Name {
			return column,nil
		}
	}
	return Column{},fmt.Errorf("`%s` not defind in table `%s`", path, table)
}

func (t *DbManager) verifyColumnData(primaryKey PrimaryKey, column Column, value interface{}) (interface{},error) {
	var data interface{}
	var err error

	if column.NotNull && value == nil {
		if primaryKey.AutoIncrement && column.Name == primaryKey.Column {
			return value,nil
		}
		return data, fmt.Errorf("column `%s` data is null", column.Name)
	}

	data,err = t.ConvertColumnData(column, value); if err != nil {
		return data,err
	}

	return data,nil
}

func (t *DbManager) verifyForeignKeys(stub shim.ChaincodeStubInterface, foreignKeys []ForeignKey, column Column, value interface{}) error {
	match,foreignKey := t.MatchForeignKeyByKey(foreignKeys, column.Name)
	if value != nil && match {
		idValue,_ := t.ConvertString(value)
		if err := t.validateRowIsNotNull(stub, foreignKey.Reference.Table, idValue); err != nil {
			return fmt.Errorf("foreignKey `%s` data `%s` not exists in table `%s`", column.Name, idValue, foreignKey.Reference.Table)
		}
	}
	return nil
}

func (t *DbManager) verifyReferenceByDelRow(stub shim.ChaincodeStubInterface, table Table, id string) error {
	foreignKey,err := t.getForeignKeyByReference(stub, table.Name, table.PrimaryKey.Column); if err != nil {
		return err
	}

	if foreignKey.ForeignKey.Table != "" {
		idValue,err := t.getRowIdByIndex(stub, foreignKey.ForeignKey.Table, foreignKey.ForeignKey.Column, id); if err != nil {
			return err
		}
		if idValue != "" {
			return fmt.Errorf("table `%s` column `%s` reference table `%s` column `%s` data `%s` ", foreignKey.ForeignKey.Table, foreignKey.ForeignKey.Column,
				foreignKey.Reference.Table, foreignKey.Reference.Column, id)
		}
	}
	return nil
}

func (t *DbManager) verifyReferenceByDelTable(stub shim.ChaincodeStubInterface, table Table) error {
	foreignKey,err := t.getForeignKeyByReference(stub, table.Name, table.PrimaryKey.Column); if err != nil {
		return err
	}

	if foreignKey.ForeignKey.Table != "" {
		return fmt.Errorf("table `%s` reference table `%s`", table.Name, foreignKey.ForeignKey.Table)
	}
	return nil
}

func (t *DbManager) verifyReferenceBySetTable(stub shim.ChaincodeStubInterface, table Table) ([]ReferenceForeignKey,[]ReferenceForeignKey,error) {
	var addForeignKeys []ReferenceForeignKey
	var deleteForeignKeys []ReferenceForeignKey
	oldTable,err := t.QueryTable(stub, table.Name); if err != nil {
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
				deleteForeignKeys = append(addForeignKeys, ReferenceForeignKey{foreignKey.Reference,ReferenceKey{table.Name,foreignKey.Column}})
			}
			if changeType > 0 {
				addForeignKeys = append(addForeignKeys, ReferenceForeignKey{foreignKey.Reference,ReferenceKey{table.Name,foreignKey.Column}})
			}
		}

	}

	return addForeignKeys,deleteForeignKeys,nil
}

func (t *DbManager) validateTableExists(stub shim.ChaincodeStubInterface, tableName string) error {
	bytes,err := t.getTableDataByFilter(stub, tableName,true)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return fmt.Errorf("table `%s` not exists", tableName)
	}
	return nil
}

func (t *DbManager) validateTableNotExists(stub shim.ChaincodeStubInterface, tableName string) error {
	bytes,err := t.getTableDataByFilter(stub, tableName,true)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("table `%s` already exists", tableName)
	}
	return nil
}

func (t *DbManager) validateQueryTableIsNotNull(stub shim.ChaincodeStubInterface, tableName string) (Table,error) {
	data,err := t.QueryTable(stub, tableName)
	if err != nil {
		return data,err
	}
	if data.Columns == nil && len(data.Columns) == 0 {
		return data,fmt.Errorf("table `%s` is null", tableName)
	}

	return data,nil
}

func (t *DbManager) validateRowExists(stub shim.ChaincodeStubInterface, tableName string, id string) ([]byte,error) {
	bytes,err := t.getRowDataByFilter(stub, tableName, id,true)
	if err != nil {
		return nil,err
	}
	if len(bytes) == 0 {
		return nil,fmt.Errorf("row `%s` not exists in table `%s`", id, tableName)
	}
	return bytes,nil
}

func (t *DbManager) validateRowNotExists(stub shim.ChaincodeStubInterface, tableName string, id string) error {
	bytes,err := t.getRowDataByFilter(stub, tableName, id,true)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("row `%s` already exists in table `%s`", id, tableName)
	}
	return nil
}

func (t *DbManager) validateRowIsNotNull(stub shim.ChaincodeStubInterface, tableName string, id string) error {
	bytes,err := t.getRowData(stub, tableName, id)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return fmt.Errorf("row `%s` not exists in table `%s`", id, tableName)
	}
	return nil
}

func (t *DbManager) validateRowIsNull(stub shim.ChaincodeStubInterface, tableName string, id string) error {
	bytes,err := t.getRowData(stub, tableName, id)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("row `%s` already exists in table `%s`", id, tableName)
	}
	return nil
}

func (t *DbManager) validateSchemaExists(stub shim.ChaincodeStubInterface, schemaName string) error {
	bytes,err := t.getSchemaDataByFilter(stub, schemaName,true)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return fmt.Errorf("schema `%s` not exists", schemaName)
	}
	return nil
}

func (t *DbManager) validateSchemaNotExists(stub shim.ChaincodeStubInterface, schemaName string) error {
	bytes,err := t.getSchemaDataByFilter(stub, schemaName,true)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("schema `%s` already exists", schemaName)
	}
	return nil
}

func (t *DbManager) validateQuerySchemaIsNotNull(stub shim.ChaincodeStubInterface, schemaName string) (Schema,error) {
	data,err := t.querySchema(stub, schemaName)
	if err != nil {
		return data,err
	}
	if data.LayerNum == 0 {
		return data,fmt.Errorf("schema `%s` is null", schemaName)
	}

	return data,nil
}

func (t *DbManager) verifyRow(stub shim.ChaincodeStubInterface, table Table, id string, row map[string]interface{}, op OpType) (string,string,map[string]interface{},error) {
	idKey := ""
	idValue := ""
	tableName := table.Name
	newRow := map[string]interface{}{}
	if op == UPDATE {
		version,err := t.validateRowExists(stub, tableName, id); if err != nil {
			return idKey,idValue,newRow,err
		}
		oldRow,err := t.queryRowByVersion(stub, tableName, id, version); if err != nil {
			return idKey,idValue,newRow,err
		}

		if oldRow == nil {
			return idKey,idValue,newRow,fmt.Errorf("not find rowId `%s` in table`%s` ", id, tableName)
		}
		for k,v := range row {
			oldRow[k] = v
		}
		row = oldRow
		idKey = table.PrimaryKey.Column
		idValue = id
	}else if op == ADD {
		for _,column := range table.Columns {
			if table.PrimaryKey.AutoIncrement && column.Name == table.PrimaryKey.Column {
				continue
			}
			_,ok := row[column.Name]
			if !ok {
				return idKey,idValue,newRow,fmt.Errorf("`%s` not exists in table `%s`", column.Name, tableName)
			}
		}
	}

	for k,v := range row {
		column,err := t.verifyColumn(table.Columns, k, table.Name, k); if err != nil {
			return idKey,idValue,newRow,err
		}

		value,err := t.verifyColumnData(table.PrimaryKey, column, v); if err != nil {
			return idKey,idValue,newRow,err
		}

		newRow[k] = value

		if len(table.ForeignKeys) > 0 {
			err = t.verifyForeignKeys(stub, table.ForeignKeys, column, value); if err != nil {
				return idKey,idValue,newRow,err
			}
		}
	}

	if op == ADD {
		idKey,idValue = t.getTablePrimaryKey(table, newRow)
		err := t.validateRowNotExists(stub, tableName, idValue); if err != nil {
			return idKey,idValue,newRow,err
		}
		if table.PrimaryKey.AutoIncrement {
			var rowId int64 = 0
			if newRow[idKey] != nil {
				rowId = newRow[idKey].(int64)
			}
			if rowId > 0 {
				idValue = t.Int64ToString(rowId)
			}else{
				autoId,err := t.autoIncrement(stub, tableName)
				if err != nil {
					return idKey,idValue,newRow,err
				}
				newRow[idKey] = autoId
				idValue = t.Int64ToString(autoId)
			}
		}
	}

	return idKey,idValue,newRow,nil
}