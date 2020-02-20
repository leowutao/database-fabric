package db

func (t *DbManager) putForeignKeyIndex(table Table, idValue string, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := t.ConvertString(row[column]); if err != nil {
			return err
		}
		if err := t.putRowIdIndex(table.Name, column, value, idValue); err != nil {
			return err
		}
	}

	return nil
}

func (t *DbManager) delForeignKeyIndex(table Table, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := t.ConvertString(row[column]); if err != nil {
			return err
		}
		if err := t.delRowIdIndex(table.Name, column, value); err != nil {
			return err
		}
	}

	return nil
}

func (t *DbManager) queryRowDataByIndex(tableName string, columnName string, columnData string) (string,map[string]interface{},error) {
	idValue := ""
	row := map[string]interface{}{}
	idValue,err := t.getRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return idValue,row,err
	}

	row,err = t.queryRow(tableName, idValue); if err != nil {
		return idValue,row,err
	}

	return idValue,row,nil
}

func (t *DbManager) queryRowDataListByIndex(tableName string, columnName string, columnData string) ([]string,[]map[string]interface{},error) {
	var idValues []string
	var rows []map[string]interface{}
	idValues,err := t.getAllRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return idValues,rows,err
	}

	for _,idValue := range idValues {
		row,err := t.queryRow(tableName, idValue); if err != nil {
			return idValues,rows,err
		}
		rows = append(rows, row)
	}

	return idValues,rows,nil
}