package util

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
)

func MatchForeignKeyByKey(foreignKeys []db.ForeignKey, key string) (bool,db.ForeignKey) {
	for _,v := range foreignKeys {
		if v.Column == key {
			return true,v
		}
	}
	return false,db.ForeignKey{}
}

func MatchForeignKeyByTable(foreignKeys []db.ForeignKey, tableName string) (bool,db.ForeignKey) {
	for _,v := range foreignKeys {
		if v.Reference.Table == tableName {
			return true,v
		}
	}
	return false,db.ForeignKey{}
}

func GetTablePrimaryKey(table db.Table, row map[string]interface{}) (string,string) {
	primaryKey := table.PrimaryKey.Column
	value := row[primaryKey]
	valueStr,_ := ConvertString(value)
	primaryValue := valueStr
	return primaryKey,primaryValue
}

func VerifyColumn(columns []db.Column, k string, table string, path string) (db.Column,error) {
	for _, column := range columns {
		if k == column.Name {
			return column,nil
		}
	}
	return db.Column{},fmt.Errorf("`%s` not defind in table `%s`", path, table)
}

func VerifyColumnData(primaryKey db.PrimaryKey, column db.Column, value interface{}) (interface{},error) {
	var data interface{}
	var err error

	if column.NotNull && value == nil {
		if primaryKey.AutoIncrement && column.Name == primaryKey.Column {
			return value,nil
		}
		return data, fmt.Errorf("column `%s` data is null", column.Name)
	}

	data,err = ConvertColumnData(column, value); if err != nil {
		return data,err
	}

	return data,nil
}