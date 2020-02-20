package db

import (
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	RecursionLayer = 10
	JsonArrayType = "[]interface {}"
	JsonObjectType = "map[string]interface {}"
	JsonArrayObjectType = "[]" + JsonObjectType
)

type Row struct {
	Table string `json:"table"`
	IdKey string `json:"idKey"`
	IdValue string `json:"idValue"`
	Data map[string]interface{} `json:"data"`
}

type SchemaRow struct {
	Row Row `json:"row"`
	Rows []Row `json:"rows"`
}

type RecursionData struct {
	PrevPath string`json:"prevPath"`
	PrevTable string`json:"prevTable"`
	PrevId string`json:"prevId"`
	Data interface{} `json:"data"`
	Model Model `json:"model"`
}

////////////////// Public Function //////////////////
func (t *DbManager) AddSchemaByJson(schemaJson string) (string,error) {
	if schemaJson == "" {
		return "",fmt.Errorf("schemaJson is null")
	}
	var schema Schema
	if err := json.Unmarshal([]byte(schemaJson), &schema); err != nil {
		return "",err
	}
	if schema.Name == "" {
		return "",fmt.Errorf("name is null")
	}
	if err := t.validateSchemaNotExists(schema.Name); err != nil {
		return "",err
	}
	return schema.Name,t.setSchema(schema)
}

func (t *DbManager) UpdateSchemaByJson(schemaJson string) (string,error) {
	if schemaJson == "" {
		return "",fmt.Errorf("schemaJson is null")
	}
	var schema Schema
	if err := json.Unmarshal([]byte(schemaJson), &schema); err != nil {
		return "",err
	}
	if schema.Name == "" {
		return "",fmt.Errorf("name is null")
	}
	if err := t.validateSchemaExists(schema.Name); err != nil {
		return "",err
	}
	return schema.Name,t.setSchema(schema)
}

func (t *DbManager) QuerySchemaBytes(schemaName string) ([]byte,error) {
	return t.getSchemaData(schemaName)
}

func (t *DbManager) QueryAllSchemaNameBytes() ([]byte,error) {
	tables,err := t.getAllSchemaKey(); if err != nil {
		return nil,err
	}
	return t.ConvertJsonBytes(tables)
}

func (t *DbManager) AddSchemaRowByJson(schemaName string, dataJson string) ([]string,[]Row,error) {
	var ids []string
	var rows []Row
	var err error
	if schemaName == "" {
		return ids,rows,fmt.Errorf("schemaName is null")
	}
	if dataJson == "" {
		return ids,rows,fmt.Errorf("dataJson is null")
	}
	var data interface{}
	if err = json.Unmarshal([]byte(dataJson), &data); err != nil {
		return ids,rows,err
	}
	return t.setSchemaRow(schemaName,"", data, ADD)
}

func (t *DbManager) UpdateSchemaRowByJson(schemaName string, id string, dataJson string) ([]string,[]Row,error) {
	var ids []string
	var rows []Row
	var err error
	if schemaName == "" {
		return ids,rows,fmt.Errorf("schemaName is null")
	}
	if id == "" {
		return ids,rows,fmt.Errorf("id is null")
	}
	if dataJson == "" {
		return ids,rows,fmt.Errorf("dataJson is null")
	}
	var data interface{}
	if err = json.Unmarshal([]byte(dataJson), &data); err != nil {
		return ids,rows,err
	}
	return t.setSchemaRow(schemaName, id, data, UPDATE)
}

func (t *DbManager) DelSchema(schemaName string) error {
	if schemaName == "" {
		return fmt.Errorf("schemaName is null")
	}
	_,err := t.validateQuerySchemaIsNotNull(schemaName)
	if err != nil {
		return err
	}
	return t.delSchemaData(schemaName)
}

func (t *DbManager) DelSchemaRow(schemaName string, id string) (map[string][]map[string]interface{},error) {
	if schemaName == "" {
		return nil,fmt.Errorf("schemaName is null")
	}
	if id == "" {
		return nil,fmt.Errorf("id is null")
	}
	schema,err := t.validateQuerySchemaIsNotNull(schemaName)
	if err != nil {
		return nil,err
	}
	_,tableRows,err := t.recursionModelQueryRow(schema.LayerNum, 0, RecursionData{"",schema.Model.Table,id,nil,schema.Model})
	if err != nil {
		return tableRows,err
	}
	for table,rows := range tableRows {
		for _,row := range rows {
			err := t.delRowByObj(table, row); if err != nil {
				return tableRows,err
			}
		}
	}
	return tableRows,nil
}

func (t *DbManager)QuerySchemaRowByWithPaginationBytes(schemaName string, id string, pageSize int32) ([]byte,error) {
	pagination,err := t.querySchemaRowByWithPagination(schemaName, id, pageSize); if err != nil {
		return nil,err
	}
	return t.ConvertJsonBytes(pagination)
}

func (t *DbManager) QuerySchemaRowDemo(schemaName string) (interface{},error) {
	schema,err := t.validateQuerySchemaIsNotNull(schemaName)
	if err != nil {
		return nil,err
	}

	row,_,err := t.recursionModelQueryRow(schema.LayerNum, 0, RecursionData{"",schema.Model.Table,"",nil,schema.Model})
	if err != nil {
		return row,err
	}

	return row,nil
}

func (t *DbManager) QuerySchemaRow(schemaName string, id string) (map[string]interface{},error) {
	if schemaName == "" {
		return nil,fmt.Errorf("schemaName is null")
	}
	if id == "" {
		return nil,fmt.Errorf("id is null")
	}
	schema,err := t.validateQuerySchemaIsNotNull(schemaName)
	if err != nil {
		return nil,err
	}

	return t.recursionSchemaRow(schema, id, RecursionData{"",schema.Model.Table,id,nil,schema.Model})
}

////////////////// Private Function //////////////////
func (t *DbManager) setSchema(schema Schema) error {
	if schema.Name == "" {
		return fmt.Errorf("name is null")
	}

	layerNum,err := t.recursionModel(0, schema.Model)
	if err != nil {
		return err
	}

	schema.LayerNum = layerNum
	schemaByte,err := t.ConvertJsonBytes(schema)
	if err != nil {
		return err
	}

	if err = t.putSchemaData(schema.Name, schemaByte); err != nil {
		return err
	}

	return nil
}

func (t *DbManager) recursionModel(layerNum int8, model Model) (int8,error) {
	layerNum++
	if layerNum > RecursionLayer {
		return layerNum,fmt.Errorf("model recursion max layerNum `%d`", RecursionLayer)
	}

	if model.Name == "" {
		return layerNum,fmt.Errorf("model name is null")
	}
	if model.Table == "" {
		return layerNum,fmt.Errorf("model table is null")
	}

	tableName := model.Table
	if err:= t.validateTableExists(tableName); err !=nil {
		return layerNum,err
	}

	if len(model.Models) > 0 {
		var maxLayerNum int8 = 0
		modeNames := map[string]bool{}
		for _,subModel := range model.Models{
			_, exists := modeNames[subModel.Name]
			if exists {
				return layerNum,fmt.Errorf("modelName `%s` exists", subModel.Name)
			}
			modeNames[subModel.Name] = true

			subTable,err := t.validateQueryTableIsNotNull(subModel.Table)
			if err != nil {
				return layerNum,err
			}
			match,_ := t.MatchForeignKeyByTable(subTable.ForeignKeys, tableName); if !match {
				return layerNum,fmt.Errorf("table `%s` foreignKey not find table `%s` relation", subTable.Name, tableName)
			}
			currentLayerNum,err := t.recursionModel(layerNum, subModel)
			if err != nil {
				return layerNum,err
			}
			if currentLayerNum > maxLayerNum {
				maxLayerNum = currentLayerNum
			}
		}
		return maxLayerNum,nil
	}
	return layerNum,nil
}
func (t *DbManager) querySchema(schemaName string) (Schema,error) {
	schema := Schema{}
	schemaBytes,err := t.getSchemaData(schemaName)
	if err != nil {
		return schema,err
	}
	if len(schemaBytes) > 0 {
		err = json.Unmarshal(schemaBytes, &schema)
		if err != nil {
			return schema,err
		}
	}
	return schema,nil
}

func (t *DbManager) setSchemaRow(schemaName string, id string, data interface{}, op OpType) ([]string,[]Row,error) {
	var ids []string
	var rows []Row
	var err error

	schema,err := t.validateQuerySchemaIsNotNull(schemaName)
	if err != nil {
		return ids,rows,err
	}

	schemaRows,err := t.recursionJsonData(schema.LayerNum,0, id, op, RecursionData{"",schema.Model.Table,"",data, schema.Model})
	if err != nil {
		return ids,rows,err
	}
	if schemaRows == nil || len(schemaRows) == 0 {
		return ids,rows,fmt.Errorf("rows is null")
	}

	for _,schemaRow := range schemaRows {
		ids = append(ids, schemaRow.Row.IdValue)
		rows = append(rows, schemaRow.Row)
		for _,row := range schemaRow.Rows {
			rows = append(rows, row)
		}
	}

	return ids,rows,err
}

func (t *DbManager) recursionJsonData(schemaLayerNum int8, layerNum int8, id string, op OpType, recursionData RecursionData) ([]SchemaRow,error) {
	prevPath,prevTable,prevId,data,model := recursionData.PrevPath,recursionData.PrevTable,recursionData.PrevId,recursionData.Data,recursionData.Model
	var rows []SchemaRow
	layerNum++
	if layerNum > RecursionLayer {
		return rows,fmt.Errorf("layerNum `%d` overstep, recursion max layerNum `%d`", layerNum, RecursionLayer)
	}
	if layerNum > schemaLayerNum {
		return rows,fmt.Errorf("layerNum `%d` overstep, schema max layerNum `%d`", layerNum, schemaLayerNum)
	}

	var list []map[string]interface{}
	dataType := reflect.ValueOf(data).Type().String()
	if model.IsArray {
		if dataType == JsonArrayObjectType {
			for _, value := range data.([]map[string]interface{}) {
				list = append(list, value)
			}
		}else if dataType == JsonArrayType {
			for _, value := range data.([]interface{}) {
				valueType := reflect.ValueOf(value).Type().String()
				if valueType != JsonObjectType {
					return rows,fmt.Errorf("`%s` array value is not JsonObjectType", prevPath)
				}
				list = append(list, value.(map[string]interface{}))
			}
		}else{
			return rows,fmt.Errorf("`%s` type is not JsonArrayType", prevPath)
		}
	}else{
		if dataType != JsonObjectType {
			return rows,fmt.Errorf("`%s` type is not JsonObjectType", prevPath)
		}
		list = append(list, data.(map[string]interface{}))
	}

	table,err := t.validateQueryTableIsNotNull(model.Table)
	if err != nil {
		return rows,err
	}

	var foreignKey ForeignKey
	if prevId != "" {
		match := false
		match,foreignKey = t.MatchForeignKeyByTable(table.ForeignKeys, prevTable); if !match {
			return rows,fmt.Errorf("table `%s` foreignKey not find table `%s` relation", table.Name, prevTable)
		}
	}

	modelsCount := len(model.Models)
	var subRecursionDataList []RecursionData
	for _,row := range list {
		schemaRow := SchemaRow{}
		matchModelCount := 0
		newRow := map[string]interface{}{}
		if prevId != "" {
			row[foreignKey.Column] = prevId
		}
		for k,v := range row {
			var path = k
			if prevPath != "" {
				path = prevPath + "~" + path
			}
			match,subModel := t.matchModel(model.Models, k, model.Name)
			if match {
				matchModelCount++
				subRecursionDataList = append(subRecursionDataList, RecursionData{path,"","",v, subModel})
			}else {
				newRow[k] = v
			}
		}
		if matchModelCount != modelsCount {
			return rows,fmt.Errorf("matchModelCount `%d` not equal modelsCount `%d`", matchModelCount, modelsCount)
		}

		if id == "" {
			id,err = t.ConvertString(newRow[table.PrimaryKey.Column]); if err != nil {
				return rows,err
			}
		}

		idKey,idValue,newRow,err := t.verifyRow(table, id, newRow, op)
		if err != nil {
			return rows,err
		}

		if err := t.putRow(table, idKey, idValue, newRow, op); err != nil {
			return rows,err
		}

		schemaRow.Row = Row{table.Name, idKey,idValue,newRow}

		if subRecursionDataList != nil {
			for _,subRecursionData := range subRecursionDataList {
				subRecursionData.PrevTable = table.Name
				subRecursionData.PrevId = idValue
				subSchemaRows,err := t.recursionJsonData(schemaLayerNum, layerNum,"", op, subRecursionData)
				if err != nil {
					return rows,err
				}
				for _,subSchemaRow := range subSchemaRows {
					schemaRow.Rows = append(schemaRow.Rows, subSchemaRow.Row)
					for _,subRow := range subSchemaRow.Rows {
						schemaRow.Rows = append(schemaRow.Rows, subRow)
					}
				}
			}
		}
		rows = append(rows, schemaRow)
	}

	return rows,nil
}

func (t *DbManager) matchModel(models []Model, key string, modelName string) (bool,Model) {
	for _, model := range models {
		if key == model.Name {
			return true,model
		}
	}
	return false,Model{}
}



func (t *DbManager)querySchemaRowByWithPagination(schemaName string, id string, pageSize int32) (Pagination,error) {
	pagination := Pagination{}
	schema,err := t.validateQuerySchemaIsNotNull(schemaName)
	if err != nil {
		return pagination,err
	}

	table,err := t.validateQueryTableIsNotNull(schema.Model.Table)
	if err != nil {
		return pagination,err
	}

	rowPagination,err := t.queryRowWithPagination(table.Name, id, pageSize); if err !=nil {
		return pagination,err
	}
	var values []interface{}
	for _,data := range rowPagination.List {
		row := data.(map[string]interface{})
		rowId,err := t.ConvertString(row[table.PrimaryKey.Column]); if err !=nil {
			return pagination,err
		}
		data,err := t.recursionSchemaRow(schema, id, RecursionData{"",table.Name,rowId,row,schema.Model})
		if err != nil {
			return pagination,err
		}
		values = append(values, data)
	}
	return t.Pagination(rowPagination.PageSize, rowPagination.Total, values),nil
}

func (t *DbManager) recursionSchemaRow(schema Schema, id string, recursionData RecursionData) (map[string]interface{},error) {
	row,_,err := t.recursionModelQueryRow(schema.LayerNum, 0, recursionData)
	if err != nil {
		return nil,err
	}
	if schema.Model.IsArray {
		rows := row.([]map[string]interface{})
		if len(rows) > 0 {
			row = rows[0]
		}
	}
	return row.(map[string]interface{}),nil
}

func (t *DbManager) recursionModelQueryRow(schemaLayerNum int8, layerNum int8, recursionData RecursionData) (interface{},map[string][]map[string]interface{},error) {
	prevTable,prevId,model,data := recursionData.PrevTable,recursionData.PrevId,recursionData.Model,recursionData.Data
	var idValues []string
	var rows []map[string]interface{}
	layerNum++
	if layerNum > schemaLayerNum {
		return nil,nil,fmt.Errorf("model recursion max layerNum `%d`", schemaLayerNum)
	}

	if prevId != "" {
		if layerNum == 1 {
			if data != nil {
				dataType := reflect.ValueOf(data).Type().String()
				if dataType != JsonObjectType {
					return nil,nil,fmt.Errorf("recursionData data type is not JsonObjectType")
				}
				rows = append(rows,  data.(map[string]interface{}))
			}else{
				row,err := t.queryRow(model.Table, prevId); if err != nil {
					return nil,nil,err
				}
				rows = append(rows, row)
			}
			idValues = append(idValues, prevId)
		}else{
			table,err := t.validateQueryTableIsNotNull(model.Table)
			if err != nil {
				return nil,nil,err
			}
			match,foreignKey := t.MatchForeignKeyByTable(table.ForeignKeys, prevTable); if !match {
				return nil,nil,fmt.Errorf("table `%s` foreignKey not find table `%s` relation", table.Name, prevTable)
			}
			if model.IsArray {
				idValues,rows, err = t.queryRowDataListByIndex(model.Table, foreignKey.Column, prevId); if err != nil {
					return nil,nil,err
				}
			}else{
				idValue,row,err := t.queryRowDataByIndex(model.Table, foreignKey.Column, prevId); if err != nil {
					return nil,nil,err
				}
				idValues = append(idValues, idValue)
				rows = append(rows, row)
			}
		}
	}else{
		row,err := t.QueryRowDemo(model.Table); if err != nil {
			return nil,nil,err
		}
		idValues = append(idValues, "")
		rows = append(rows, row)
	}

	var newRows []map[string]interface{}
	tableRows := map[string][]map[string]interface{}{}
	tableRows[model.Table] = rows
	if len(model.Models) > 0 {
		for i,row := range rows {
			if row == nil {
				continue
			}
			idValue := idValues[i]
			for _,subModel := range model.Models{
				subRow,subTableRows,err := t.recursionModelQueryRow(schemaLayerNum, layerNum, RecursionData{"",model.Table,idValue,nil,subModel}); if err != nil {
					return nil,nil,err
				}
				row[subModel.Name] = subRow
				for k,v := range subTableRows {
					r,ok := tableRows[k]
					if ok {
						tableRows[k] = append(r, v...)
					}else{
						tableRows[k] = v
					}
				}
			}
			newRows = append(newRows, row)
		}
	}else{
		newRows = rows
	}

	if model.IsArray {
		return newRows,tableRows,nil
	}else{
		row := map[string]interface{}{}
		if len(newRows) > 0{
			row = newRows[0]
		}
		return row,tableRows,nil
	}
}