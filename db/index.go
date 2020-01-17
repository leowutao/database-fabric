package db

import (
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

func (t *DbManager) putForeignKeyIndex(stub shim.ChaincodeStubInterface, table Table, idValue string, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := t.ConvertString(row[column]); if err != nil {
			return err
		}
		if err := t.putRowIdIndex(stub, table.Name, column, value, idValue); err != nil {
			return err
		}
	}

	return nil
}

func (t *DbManager) delForeignKeyIndex(stub shim.ChaincodeStubInterface, table Table, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := t.ConvertString(row[column]); if err != nil {
			return err
		}
		if err := t.delRowIdIndex(stub, table.Name, column, value); err != nil {
			return err
		}
	}

	return nil
}

func (t *DbManager) queryRowDataByIndex(stub shim.ChaincodeStubInterface, tableName string, columnName string, columnData string) (string,map[string]interface{},error) {
	idValue := ""
	row := map[string]interface{}{}
	idValue,err := t.getRowIdByIndex(stub, tableName, columnName, columnData); if err != nil {
		return idValue,row,err
	}

	row,err = t.queryRow(stub, tableName, idValue); if err != nil {
		return idValue,row,err
	}

	return idValue,row,nil
}

func (t *DbManager) queryRowDataListByIndex(stub shim.ChaincodeStubInterface, tableName string, columnName string, columnData string) ([]string,[]map[string]interface{},error) {
	var idValues []string
	var rows []map[string]interface{}
	idValues,err := t.getAllRowIdByIndex(stub, tableName, columnName, columnData); if err != nil {
		return idValues,rows,err
	}

	for _,idValue := range idValues {
		row,err := t.queryRow(stub, tableName, idValue); if err != nil {
			return idValues,rows,err
		}
		rows = append(rows, row)
	}

	return idValues,rows,nil
}