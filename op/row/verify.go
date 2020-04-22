package row

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

/**
	行数据为空
 */
func (operation *RowOperation) validateNull(table *db.TableData, rowID db.RowID) error {
	blockID,err := operation.iDatabase.QueryRowBlockID(table, rowID); if err != nil {
		return err
	}
	if blockID == 0 {
		return fmt.Errorf("row `%d` not exists in table `%s`", rowID, table.Name)
	}
	return nil
}

/**
	行数据已存在
*/
func (operation *RowOperation) validateExists(table *db.TableData, rowID db.RowID) error {
	blockID,err := operation.iDatabase.QueryRowBlockID(table, rowID); if err != nil {
		return err
	}
	if blockID > 0 {
		return fmt.Errorf("row `%d` already exists in table `%s`", rowID, table.Name)
	}
	return nil
}

/**
	验证表中行数据必须不为空，并获取行数据
*/
func (operation *RowOperation) validateNullOfData(table *db.TableData, rowID db.RowID) (*db.RowData,error) {
	row,err := operation.iDatabase.QueryRowData(table, rowID); if err != nil {
		return nil,err
	}
	if row == nil {
		return nil,fmt.Errorf("row `%d` is null in table `%s`", rowID, table.Name)
	}
	return row,nil
}

/**
	外建匹配行数据已存在
*/
func (operation *RowOperation) validateExistsByForeignKey(table *db.TableData, foreignKey db.ForeignKey, referenceRowID db.RowID) error {
	rows,err := operation.iDatabase.QueryRowIDByForeignKey(table.Id, foreignKey, referenceRowID); if err != nil {
		return err
	}
	if len(rows) > 0 {
		return fmt.Errorf("reference row `%d` already exists in table `%s`", referenceRowID, table.Name)
	}
	return nil
}

/**
	json数据格式化行数据(新增、修改、删除操作)
	1、验证数据类型，2、序列化数据，3、验证外建约束，4、列数据组装成行
*/
func (operation *RowOperation) FormatRowData(table *db.Table, rowJson db.JsonData, op db.OpType) (*db.RowData,error) {
	primaryColumn := table.Data.Columns[table.Data.PrimaryKey.ColumnID-1]
	id,exists := rowJson[primaryColumn.Name]
	rowID := db.RowID(0)
	if exists {
		var err error
		rowID,err = util.ConvertRowID(id); if err != nil {
			return  nil,err
		}
	}
	row := &db.RowData{Id:rowID, Op:op}
	if op == db.UPDATE || op == db.DELETE {
		if row.Id == 0 {
			return nil,fmt.Errorf("update or delete row must rowID")
		}
		oldRow,err := operation.validateNullOfData(table.Data, row.Id); if err != nil {
			return nil,err
		}
		row.Data = oldRow.Data
		oldRow = nil
	}else if op == db.ADD {
		if row.Id == 0 && !table.Data.PrimaryKey.AutoIncrement {//非自增
			return nil,fmt.Errorf("add row must rowID or set autoIncrement=true")
		}else{
			if err := operation.validateExists(table.Data, row.Id); err != nil {
				return nil,err
			}
		}
	}

	if op == db.ADD || op == db.UPDATE {
		if err := operation.formatAddOrUpdateRowData(table, rowJson, row); err != nil {
			return nil,err
		}
	}else if op == db.DELETE {
		if err := operation.verifyDeleteRowData(table, row.Id); err != nil {
			return nil,err
		}
	}
	return row,nil
}

/**
	格式化添加或修改行数据
 */
func (operation *RowOperation) formatAddOrUpdateRowData(table *db.Table, rowJson db.JsonData, row *db.RowData) error {
	//列数据验证和序列化
	for i,column := range table.Data.Columns {
		var data []byte
		if i < len(row.Data) {//获取原行中列值
			data = row.Data[i]
		}
		if column.Id != table.Data.PrimaryKey.ColumnID && !column.IsDeleted { //过滤主键和删除列
			value, ok := rowJson[column.Name] //匹配待写入列值
			if ok { //待写入列值序列化字节
				var err error
				data, err = util.FormatColumnData(column, value);
				if err != nil {
					return err
				}
			}
			if data == nil { //未设置值验证必填或设置默认值
				if column.NotNull { //是否必填
					return fmt.Errorf("table `%s` column `%s` value is not null", table.Data.Name, column.Name)
				} else { //默认值
					data = column.Default
					ok = true // 有默认值设置为待写入
				}
			}
			//外建约束验证
			if ok { //待写入列值需要验证
				if err := operation.verifyForeignKey(table, column.Id, util.BytesToRowID(data)); err != nil {
					return err
				}
			}
		}
		//列数据组装
		if i < len(row.Data) {
			row.Data[i] = data
		}else{
			row.Data = append(row.Data, data)
		}
	}
	return nil
}

/**
	验证外建约束
 */
func (operation *RowOperation) verifyForeignKey(table *db.Table, columnID db.ColumnID, referenceRowID db.RowID) error {
	foreignKey,exists := table.ForeignKeys[columnID]
	if exists {
		tableName,err := operation.iDatabase.GetTableName(foreignKey.Reference.TableID); if err != nil {
			return err
		}
		referenceTable := &db.TableData{Id:foreignKey.Reference.TableID,Name:tableName,PrimaryKey:db.PrimaryKey{ColumnID:foreignKey.Reference.ColumnID}}
		if err := operation.validateNull(referenceTable, referenceRowID); err != nil {
			return fmt.Errorf("foreignKey foreign table `%s` and reference Table `%s` (add or update row `%d` in table `%s` error `%s`)", table.Data.Name, referenceTable.Name, referenceRowID, table.Data.Name, err.Error())
		}
	}
	return nil
}

/**
	验证删除行数据，触发外键约束验证
*/
func (operation *RowOperation) verifyDeleteRowData(table *db.Table, rowID db.RowID) error {
	reference := db.ReferenceKey{TableID:table.Data.Id, ColumnID:table.Data.PrimaryKey.ColumnID}
	relationKeys,err := operation.iDatabase.GetRelationKeysByReference(reference); if err != nil {
		return nil
	}
	for _,key := range relationKeys {
		tableName,err := operation.iDatabase.GetTableName(key.TableID); if err != nil {
			return err
		}
		foreignTable := &db.TableData{Id:key.TableID,Name:tableName,PrimaryKey:db.PrimaryKey{ColumnID:key.ForeignKey.ColumnID}}
		if err := operation.validateExistsByForeignKey(foreignTable, key.ForeignKey, rowID); err != nil {
			return fmt.Errorf("foreignKey foreign table `%s` and reference Table `%s` (delete row `%d` in table `%s` error `%s`)", foreignTable.Name, table.Data.Name, rowID, table.Data.Name, err.Error())
		}
	}
	return nil
}