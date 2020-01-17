package db

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	//"github.com/hyperledger/fabric/common/attrmgr"
)

////////////////// Public Function //////////////////
func (t *DbManager) AddTableByJson(stub shim.ChaincodeStubInterface, tableJson string) (string,error) {
	if tableJson == "" {
		return "",fmt.Errorf("tableJson is null")
	}
	var table Table
	if err := json.Unmarshal([]byte(tableJson), &table); err != nil {
		return "",fmt.Errorf("tableJson %s", err)
	}
	if table.Name == "" {
		return "",fmt.Errorf("name is null")
	}
	if err := t.validateTableNotExists(stub, table.Name); err != nil {
		return "",err
	}
	return table.Name,t.setTable(stub, table)
}

func (t *DbManager) UpdateTableByJson(stub shim.ChaincodeStubInterface, tableJson string) (string,error) {
	if tableJson == "" {
		return "",fmt.Errorf("tableJson is null")
	}
	var table Table
	if err := json.Unmarshal([]byte(tableJson), &table); err != nil {
		return "",fmt.Errorf("tableJson %s", err)
	}
	if table.Name == "" {
		return "",fmt.Errorf("name is null")
	}
	if err := t.validateTableExists(stub, table.Name); err != nil {
		return "",err
	}
	return table.Name,t.setTable(stub, table)
}

func (t *DbManager) DelTable(stub shim.ChaincodeStubInterface, tableName string) error {
	if tableName == "" {
		return fmt.Errorf("tableName is null")
	}
	table,err := t.validateQueryTableIsNotNull(stub, tableName); if err != nil {
		return err
	}

	err = t.verifyReferenceByDelTable(stub, table); if err != nil {
		return err
	}

	err = t.delTableData(stub, tableName); if err != nil {
		return err
	}

	return nil
}

func (t *DbManager) QueryTableBytes(stub shim.ChaincodeStubInterface, tableName string) ([]byte,error) {
	return t.getTableData(stub, tableName)
}

func (t *DbManager) QueryAllTableNameBytes(stub shim.ChaincodeStubInterface) ([]byte,error) {
	tables,err := t.getAllTableKey(stub); if err != nil {
		return nil,err
	}
	return t.ConvertJsonBytes(tables)
}

func (t *DbManager) QueryTable(stub shim.ChaincodeStubInterface, tableName string) (Table,error) {
	table := Table{}
	tableBytes,err := t.getTableData(stub, tableName)
	if err != nil {
		return table,err
	}
	if len(tableBytes) > 0 {
		err = json.Unmarshal(tableBytes, &table)
		if err != nil {
			return table,err
		}
	}
	return table,nil
}

////////////////// Private Function /////////////////
func (t *DbManager) setTable(stub shim.ChaincodeStubInterface, table Table) error {
	if table.Name == "" {
		return fmt.Errorf("name is null")
	}
	if table.Columns == nil || len(table.Columns) == 0 {
		return fmt.Errorf("columns key is null")
	}
	if len(table.PrimaryKey.Column) == 0 {
		return fmt.Errorf("primaryKey column is null")
	}

	if table.PrimaryKey.AutoIncrement {
		key := table.PrimaryKey.Column
		column,err := t.verifyColumn(table.Columns, key, table.Name, key); if err != nil {
			return fmt.Errorf("primaryKey keys `%s` not found in columns", key)
		}
		if column.Type != INT {
			return fmt.Errorf("primaryKey autoIncrement keys `%s` type must is INT", key)
		}
	}else{
		key := table.PrimaryKey.Column
		column,err := t.verifyColumn(table.Columns, key, table.Name, key); if err != nil {
			return fmt.Errorf("primaryKey keys `%s` not found in columns", key)
		}
		if column.Type != INT && column.Type != VARCHAR {
			return fmt.Errorf("primaryKey keys `%s` type must is INT or VARCHAR", key)
		}
	}

	if table.ForeignKeys != nil {
		for _,foreignKey := range table.ForeignKeys {
			key := foreignKey.Column
			_,err := t.verifyColumn(table.Columns, key, table.Name, key); if err != nil {
				return fmt.Errorf("foreignKey key `%s` not found in columns", key)
			}
			relationTable,err := t.validateQueryTableIsNotNull(stub, foreignKey.Reference.Table); if err != nil {
				return err
			}
			match,relationForeignKey := t.MatchForeignKeyByTable(relationTable.ForeignKeys, table.Name); if match {
				return fmt.Errorf("table `%s` and `%s` foreignKey realtion exists", table.Name, relationForeignKey.Reference.Table)
			}
		}
	}

	var newColumns []Column
	for _,column := range table.Columns {
		value,err := t.ConvertColumnData(column, column.Default); if err != nil {
			return err
		}

		column.Default = value
		newColumns = append(newColumns, column)
	}

	table.Columns = newColumns
	tableByte,err := t.ConvertJsonBytes(table)
	if err != nil {
		return err
	}

	if err = t.setForeignKey(stub, table); err != nil {
		return err
	}

	if err = t.putTableData(stub, table.Name, tableByte); err != nil {
		return err
	}

	return nil
}

func (t *DbManager) setForeignKey(stub shim.ChaincodeStubInterface, table Table) error {
	addForeignKeys,deleteForeignKeys,err := t.verifyReferenceBySetTable(stub, table); if err != nil {
		return err
	}

	for _,foreignKey := range addForeignKeys {
		if err = t.putForeignKey(stub, foreignKey); err != nil {
			return err
		}
	}

	for _,foreignKey := range deleteForeignKeys {
		if err = t.delForeignKey(stub, foreignKey); err != nil {
			return err
		}
	}
	return nil
}

func (t *DbManager) getTablePrimaryKey(table Table, row map[string]interface{}) (string,string) {
	primaryKey := table.PrimaryKey.Column
	value := row[primaryKey]
	valueStr,_ := t.ConvertString(value)
	primaryValue := valueStr
	return primaryKey,primaryValue
}

func (t *DbManager) setTableTally(stub shim.ChaincodeStubInterface, tableName string, increment int64, op OpType) error {
	tableTally,err := t.getTableTally(stub, tableName); if err != nil {
		return err
	}
	if op == ADD {
		tableTally.Count = tableTally.Count + 1
		if increment > tableTally.Increment {
			tableTally.Increment = increment
		}
	}else if op == UPDATE {
		if increment > tableTally.Increment {
			tableTally.Increment = increment
		}
	}else if op == DELETE {
		tableTally.Count = tableTally.Count - 1
	}
	value,err := t.ConvertJsonBytes(tableTally); if err != nil {
		return err
	}
	return t.putTallyData(stub, tableName, value)
}

func (t *DbManager) getTableTally(stub shim.ChaincodeStubInterface, tableName string) (TableTally,error) {
	tableTally := TableTally{}
	value,err := t.getTallyData(stub, tableName); if err != nil {
		return tableTally,err
	}
	if len(value) > 0 {
		err = json.Unmarshal(value, &tableTally); if err != nil {
			return tableTally,err
		}
	}
	return tableTally,nil
}

func (t *DbManager) getTableIncrement(stub shim.ChaincodeStubInterface, tableName string) (int64,error) {
	tableTally,err := t.getTableTally(stub, tableName); if err != nil {
		return 0,err
	}

	return tableTally.Increment,nil
}

func (t *DbManager) getTableCount(stub shim.ChaincodeStubInterface, tableName string) (int64,error) {
	tableTally,err := t.getTableTally(stub, tableName); if err != nil {
		return 0,err
	}
	return tableTally.Count,nil
}