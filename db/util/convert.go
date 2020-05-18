package util

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"github.com/shopspring/decimal"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

const (
	TypeInt = "int"
	TypeInt8 = "int8"
	TypeInt16 = "int16"
	TypeInt32 = "int32"
	TypeInt64 = "int64"
	TypeFloat64 = "float64"
	TypeString = "string"
	TypeBool = "bool"
	TypeDecimal = "decimal.Decimal"
)

func PageSize(pageSize int32) int32 {
	if pageSize == 0 || pageSize > 100 {
		pageSize = 100
	}
	return pageSize
}

func Pagination(pageSize int32, total db.Total, list []db.JsonData) db.Pagination {
	return db.Pagination{PageSize:PageSize(pageSize), Total:total, List:list}
}


func DecryptData(value string) (map[string]interface{},error) {
	var mapResult map[string]interface{}
	jsonStr,err :=DecryptJson(value)
	if err != nil {
		return mapResult,err
	}
	mapResult,err =ConvertMap(jsonStr)
	if err != nil {
		return mapResult,err
	}
	return mapResult,nil
}

func DecryptJson(value string) (string,error) {
	var jsonString string
	decodeBytes, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return jsonString,err
	}
	jsonString,err = url.QueryUnescape(string(decodeBytes))
	if err != nil {
		return jsonString,err
	}
	return jsonString,nil
}

func EncodeJson(value interface{}) (string,error) {
	var jsonString string
	bytes,err :=ConvertJsonBytes(value)
	if err != nil {
		return jsonString,err
	}
	jsonString = base64.StdEncoding.EncodeToString(bytes)
	return jsonString,nil
}

func ConvertJsonString(value interface{}) (string,error) {
	jsonString := ""
	bytes,err :=ConvertJsonBytes(value)
	if err != nil {
		return jsonString,err
	}
	jsonString = string(bytes)
	return jsonString,nil
}

func ConvertJsonBytes(value interface{}) ([]byte,error) {
	return json.Marshal(value)
}

func ConvertMap(jsonStr string) (map[string]interface{},error) {
	var mapResult map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapResult)
	if err != nil {
		return mapResult,err
	}
	return mapResult,nil
}

func ConvertJsonArrayBytes(values [][]byte) []byte {
	var jsonArray [][]byte
	jsonArray = append(jsonArray, []byte("["))
	ary := bytes.Join(values, []byte(","))
	jsonArray = append(jsonArray, ary)
	jsonArray = append(jsonArray, []byte("]"))
	return bytes.Join(jsonArray, []byte{})
}

//////////////////// String Convert or Parse ////////////////////

func ConvertString(value interface{}) (string,error) {
	data := ""
	if value == nil {
		return data,nil
	}
	convertType, vType :=ConvertDataType(value)
	if convertType == db.INT {
		returnType,value,err :=ConvertInt64OrDecimal(vType, value); if err != nil {
			return data,err
		}
		if returnType == TypeInt64 {
			data =Int64ToString(value.(int64))
		}else{
			data = DecimalToString(value.(decimal.Decimal))
		}
	}else if convertType == db.DECIMAL {
		data = DecimalToString(value.(decimal.Decimal))
	}else if convertType == db.VARCHAR {
		data = value.(string)
	}else if convertType == db.BOOL {
		data =BoolToString(value.(bool))
	}else{
		return data,fmt.Errorf("`%s` not convert string", vType)
	}
	return data,nil
}

func StringToBool(value string) (bool,error) {
	if len(value) > 0 {
		boolValue,err := strconv.ParseBool(value); if err != nil {
			return boolValue,err
		}
		return boolValue,nil
	}
	return false,nil
}

func StringToInt64(value string) (int64,error) {
	if len(value) > 0 {
		int64Value,err := strconv.ParseInt(value,10,64); if err != nil {
			return int64Value,err
		}
		return int64Value,nil
	}
	return 0,nil
}

func StringToDecimal(value string) (decimal.Decimal,error) {
	if value == "" {
		return decimal.NewFromInt(0),nil
	}
	return decimal.NewFromString(value)
}

//////////////////// Bool Convert or Parse ////////////////////

func ConvertBool(vType string, value interface{}) (bool,error) {
	var data bool
	var err error
	switch vType {
	case TypeBool:
		data = value.(bool)
	case TypeString:
		if value == "" {
			data = false
		}else{
			data,err =StringToBool(value.(string)); if err != nil {
				return data, fmt.Errorf("string not convert bool")
			}
		}
	default:
		return data,fmt.Errorf("`%s` not convert bool", vType)
	}
	return data,nil
}

func BoolToString(value bool) string {
	return strconv.FormatBool(value)
}

//////////////////// Bytes Convert or Parse ////////////////////

func BytesToInt32(value []byte) int32 {
	var int32Value int32 = 0
	if len(value) > 0 {
		getBuf := bytes.NewBuffer(value)
		binary.Read(getBuf, binary.BigEndian, &int32Value)
	}
	return int32Value
}

func BytesToInt64(value []byte) int64 {
	var int64Value int64 = 0
	if len(value) > 0 {
		getBuf := bytes.NewBuffer(value)
		binary.Read(getBuf, binary.BigEndian, &int64Value)
	}
	return int64Value
}

//////////////////// Int Convert or Parse ////////////////////

func Int32ToBytes(value int32) []byte {
	return IntToBytes(value)
}

func Int64ToBytes(value int64) []byte {
	return IntToBytes(value)
}

func IntToBytes(value interface{}) []byte {
	putBuf := bytes.NewBuffer([]byte{})
	binary.Write(putBuf, binary.BigEndian, value)
	return putBuf.Bytes()
}

func UInt8ToString(value uint8) string {
	return Int64ToString(int64(value))
}

func Int64ToString(value int64) string {
	return strconv.FormatInt(value, 10)
}

func ConvertInt64(vType string, value interface{}) (int64,error) {
	var data int64
	var err error
	switch vType {
	case TypeInt:
		data = int64(value.(int))
	case TypeInt8:
		data = int64(value.(int32))
	case TypeInt16:
		data = int64(value.(int32))
	case TypeInt32:
		data = int64(value.(int32))
	case TypeInt64:
		data = value.(int64)
	case TypeFloat64:
		v := value.(float64)
		str := strings.Split(fmt.Sprintf("%v", v), ".")
		var decimalNum int64 = 0
		if len(str) == 2 {
			decimalNum,_ = StringToInt64(str[1])
		}
		data = int64(v)
		if decimalNum > 0  {
			return data,fmt.Errorf("float64 not convert int64, the float64 is decimal")
		}
	case TypeString:
		if value == "" {
			data = 0
		}else{
			data,err = StringToInt64(value.(string)); if err != nil {
				return data, fmt.Errorf("string not convert int64")
			}
		}
	default:
		return data,fmt.Errorf("`%s` not convert int64", vType)
	}
	return data,nil
}

func ConvertInt64OrDecimal(vType string, value interface{}) (string,interface{},error) {
	var data interface{}
	var err error
	data, err = ConvertInt64(vType, value); if err != nil {
		if vType == TypeFloat64 {
			data = ConvertDecimalByFloat64(vType, value.(float64))
			return TypeDecimal,data,nil
		}else{
			return vType,data,err
		}
	}
	return TypeInt64,data,nil
}

//////////////////// Decimal Convert or Parse ////////////////////

func ConvertDecimal(vType string, value interface{}) (decimal.Decimal,error) {
	var data decimal.Decimal
	var err error
	switch vType {
	case TypeDecimal:
		data = value.(decimal.Decimal)
	case TypeInt,TypeInt8,TypeInt16,TypeInt32,TypeInt64:
		convertValue, err :=ConvertInt64(vType, value); if err != nil {
		return data,fmt.Errorf("`%s` not convert decimal", vType)
	}
		data = decimal.NewFromInt(convertValue)
	case TypeFloat64:
		data =ConvertDecimalByFloat64(vType, value.(float64))
	case TypeString:
		if value == "" {
			data = decimal.NewFromInt(0)
		}else{
			data,err = decimal.NewFromString(value.(string)); if err != nil {
				return data, fmt.Errorf("string not convert decimal")
			}
		}
	default:
		return data,fmt.Errorf("`%s` not convert decimal", vType)
	}
	return data,nil
}

func ConvertDecimalByFloat64(vType string, value float64) decimal.Decimal {
	return decimal.NewFromFloat(value)
}

func DecimalToString(value decimal.Decimal) string {
	return value.String()
}

//////////////////// ID Convert or Parse ////////////////////
func DatabaseIDToString(database db.DatabaseID) string {
	return Int64ToString(int64(database))
}

func TableIDToString(table db.TableID) string {
	return Int64ToString(int64(table))
}

func ColumnIDToString(column db.ColumnID) string {
	return Int64ToString(int64(column))
}

func BlockIDToString(block db.BlockID) string {
	return Int64ToString(int64(block))
}

func RowIDToString(row db.RowID) string {
	return Int64ToString(int64(row))
}

func BlockIDToBytes(blockID db.BlockID) []byte {
	if blockID == db.BlockID(0) {
		return nil
	}
	return Int32ToBytes(blockID)
}

func BytesToBlockID(value []byte) db.BlockID {
	if len(value) == 0 {
		return db.BlockID(0)
	}
	return BytesToInt32(value)
}

func RowIDToBytes(rowID db.RowID) []byte {
	if rowID == db.RowID(0) {
		return nil
	}
	return Int64ToBytes(rowID)
}

func BytesToRowID(value []byte) db.RowID {
	if len(value) == 0 {
		return db.RowID(0)
	}
	return BytesToInt64(value)
}

func ConvertRowID(value interface{}) (db.RowID,error) {
	if value == nil {
		return db.RowID(0),nil
	}
	vType := reflect.ValueOf(value).Type().String()
	switch vType {
	case TypeInt,TypeInt8,TypeInt16,TypeInt32,TypeInt64,TypeString:
		rowID,err := ConvertInt64(vType, value); if err != nil {
		return db.RowID(0),fmt.Errorf("rowID convert failed `%s`", err.Error())
	}
		return rowID,nil
	}
	return db.RowID(0),fmt.Errorf("rowID convert type error")
}

//////////////////// Data Convert or Parse ////////////////////

func ConvertDataType(value interface{}) (db.DataType,string) {
	vType := reflect.ValueOf(value).Type().String()
	switch vType {
	case TypeInt,TypeInt8,TypeInt16,TypeInt32,TypeInt64,TypeFloat64:
		return db.INT,vType
	case TypeString:
		return db.VARCHAR,vType
	case TypeBool:
		return db.BOOL,vType
	case TypeDecimal:
		return db.DECIMAL,vType
	}
	return db.UNDEFINED,vType
}

func FormatColumnData(column db.Column, value interface{}) ([]byte,error) {
	if value == nil {
		return nil,nil
	}
	columnType := column.Type
	if columnType == db.VARCHAR {
		data,err := ConvertString(value); if err != nil {
			return nil, fmt.Errorf("column `%s` data `%s` convert string error %s", column.Name, value, err)
		}
		return []byte(data),nil
	} else{
		convertType, vType := ConvertDataType(value)
		if columnType == db.BOOL && (convertType == db.VARCHAR || convertType == db.BOOL) {
			data,err := ConvertBool(vType, value); if err != nil {
				return nil, fmt.Errorf("column `%s` data `%s` convert bool error %s", column.Name, value, err)
			}
			return []byte(BoolToString(data)),nil
		} else if columnType == db.INT && (convertType == db.VARCHAR || convertType == db.INT) {
			vType,data,err := ConvertInt64OrDecimal(vType, value); if vType != TypeInt64 || err != nil {
				return nil, fmt.Errorf("column `%s` data `%s` convert int64 error %s", column.Name, value, err)
			}
			return Int64ToBytes(data.(int64)),nil
		} else if columnType == db.DECIMAL && (convertType == db.VARCHAR || convertType == db.INT || convertType == db.DECIMAL) {
			data,err := ConvertDecimal(vType, value); if err != nil {
				return nil, fmt.Errorf("column `%s` data `%s` convert decimal error %s", column.Name, value, err)
			}
			return []byte(DecimalToString(data)),nil
		} else {
			return nil, fmt.Errorf("column `%s` datatype `%s` error", column.Name, vType)
		}
	}
}


func ParseRowData(table *db.Table, rowData *db.RowData) (db.JsonData,error) {
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
			value,err = ParseColumnDataByNull(column); if err != nil {
				return nil,err
			}
		}else {
			value,err = ParseColumnData(column, data); if err != nil {
				return nil,err
			}
		}
		row[column.Name] = value
	}
	return row,nil
}

func ParseColumnDataByNull(column db.Column) (interface{},error) {
	switch column.Type {
	case db.VARCHAR:
		return "",nil
	case db.BOOL:
		return false,nil
	case db.INT:
		return 0,nil
	case db.DECIMAL:
		return 0,nil
	}
	return nil,fmt.Errorf("column `%s` datatype parse error", column.Name)
}

func ParseColumnData(column db.Column, value []byte) (interface{},error) {
	switch column.Type {
		case db.VARCHAR:
			return string(value),nil
		case db.BOOL:
			return StringToBool(string(value))
		case db.INT:
			return BytesToInt64(value),nil
		case db.DECIMAL:
			return StringToDecimal(string(value))
	}
	return nil,fmt.Errorf("column `%s` datatype parse error", column.Name)
}

func JsonDataExists(key string, json db.JsonData) bool {
	_,exists := json[key]
	return exists
}