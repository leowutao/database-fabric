package db

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
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

func (t *DbManager) PageSize(pageSize int32) int32 {
	if pageSize == 0 || pageSize > 100 {
		pageSize = 100
	}
	return pageSize
}

func (t *DbManager) Pagination(pageSize int32, total int64, list []interface{}) Pagination {
	return Pagination{t.PageSize(pageSize),total,list}
}


func (t *DbManager) DecryptData(value string) (map[string]interface{},error) {
	var mapResult map[string]interface{}
	jsonStr,err := t.DecryptJson(value)
	if err != nil {
		return mapResult,err
	}
	mapResult,err = t.ConvertMap(jsonStr)
	if err != nil {
		return mapResult,err
	}
	return mapResult,nil
}

func (t *DbManager) DecryptJson(value string) (string,error) {
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

func (t *DbManager) EncodeJson(value interface{}) (string,error) {
	var jsonString string
	bytes,err := t.ConvertJsonBytes(value)
	if err != nil {
		return jsonString,err
	}
	jsonString = base64.StdEncoding.EncodeToString(bytes)
	return jsonString,nil
}

func (t *DbManager) ConvertJsonString(value interface{}) (string,error) {
	jsonString := ""
	bytes,err := t.ConvertJsonBytes(value)
	if err != nil {
		return jsonString,err
	}
	jsonString = string(bytes)
	return jsonString,nil
}

func (t *DbManager) ConvertJsonBytes(value interface{}) ([]byte,error) {
	return json.Marshal(value)
}

func (t *DbManager) ConvertMap(jsonStr string) (map[string]interface{},error) {
	var mapResult map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &mapResult)
	if err != nil {
		return mapResult,err
	}
	return mapResult,nil
}

func (t *DbManager) ConvertJsonArrayBytes(values [][]byte) []byte {
	var jsonArray [][]byte
	jsonArray = append(jsonArray, []byte("["))
	ary := bytes.Join(values, []byte(","))
	jsonArray = append(jsonArray, ary)
	jsonArray = append(jsonArray, []byte("]"))
	return bytes.Join(jsonArray, []byte{})
}

func (t *DbManager) StringToBool(value string) (bool,error) {
	if len(value) > 0 {
		boolValue,err := strconv.ParseBool(value); if err != nil {
			return boolValue,err
		}
		return boolValue,nil
	}
	return false,nil
}

func (t *DbManager) BoolToString(value bool) string {
	return strconv.FormatBool(value)
}

func (t *DbManager) ByteToInt32(value []byte) int32 {
	var int32Value int32 = 0
	if len(value) > 0 {
		getBuf := bytes.NewBuffer(value)
		binary.Read(getBuf, binary.BigEndian, &int32Value)
	}
	return int32Value
}

func (t *DbManager) Int32ToByte(value int32) []byte {
	putBuf := bytes .NewBuffer([]byte{})
	binary.Write(putBuf, binary.BigEndian, value)
	return putBuf.Bytes()
}

func (t *DbManager) StringToInt64(value string) (int64,error) {
	if len(value) > 0 {
		int64Value,err := strconv.ParseInt(value,10,64); if err != nil {
			return int64Value,err
		}
		return int64Value,nil
	}
	return 0,nil
}

func (t *DbManager) Int64ToString(value int64) string {
	return strconv.FormatInt(value, 10)
}

func (t *DbManager) ByteToInt64(value []byte) int64 {
	var int64Value int64 = 0
	if len(value) > 0 {
		getBuf := bytes.NewBuffer(value)
		binary.Read(getBuf, binary.BigEndian, &int64Value)
	}
	return int64Value
}

func (t *DbManager) Int64ToByte(value int64) []byte {
	putBuf := bytes .NewBuffer([]byte{})
	binary.Write(putBuf, binary.BigEndian, value)
	return putBuf.Bytes()
}


func (t *DbManager) ConvertInt64(vType string, value interface{}) (int64,error) {
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
			decimalNum,_ = t.StringToInt64(str[1])
		}
		data = int64(v)
		if decimalNum > 0  {
			return data,fmt.Errorf("float64 not convert int64, the float64 is decimal")
		}
	case TypeString:
		if value == "" {
			data = 0
		}else{
			data,err = t.StringToInt64(value.(string)); if err != nil {
				return data, fmt.Errorf("string not convert int64")
			}
		}
	default:
		return data,fmt.Errorf("`%s` not convert int64", vType)
	}
	return data,nil
}

func (t *DbManager) ConvertInt64OrDecimal(vType string, value interface{}) (string,interface{},error) {
	var data interface{}
	var err error
	data, err = t.ConvertInt64(vType, value); if err != nil {
		if vType == TypeFloat64 {
			data = t.ConvertDecimalByFloat64(vType, value.(float64))
			return TypeDecimal,data,nil
		}else{
			return vType,data,err
		}
	}
	return TypeInt64,data,nil
}

func (t *DbManager) ConvertBool(vType string, value interface{}) (bool,error) {
	var data bool
	var err error
	switch vType {
	case TypeBool:
		data = value.(bool)
	case TypeString:
		if value == "" {
			data = false
		}else{
			data,err = t.StringToBool(value.(string)); if err != nil {
				return data, fmt.Errorf("string not convert bool")
			}
		}
	default:
		return data,fmt.Errorf("`%s` not convert bool", vType)
	}
	return data,nil
}

func (t *DbManager) ConvertDecimalByFloat64(vType string, value float64) decimal.Decimal {
	return decimal.NewFromFloat(value)
}

func (t *DbManager) ConvertDecimal(vType string, value interface{}) (decimal.Decimal,error) {
	var data decimal.Decimal
	var err error
	switch vType {
	case TypeDecimal:
		data = value.(decimal.Decimal)
	case TypeInt,TypeInt8,TypeInt16,TypeInt32,TypeInt64:
		convertValue, err := t.ConvertInt64(vType, value); if err != nil {
			return data,fmt.Errorf("`%s` not convert decimal", vType)
		}
		data = decimal.NewFromInt(convertValue)
	case TypeFloat64:
		data = t.ConvertDecimalByFloat64(vType, value.(float64))
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

func (t *DbManager) ConvertDataType(value interface{}) (DataType,string) {
	vType := reflect.ValueOf(value).Type().String()
	switch vType {
	case TypeInt,TypeInt8,TypeInt16,TypeInt32,TypeInt64,TypeFloat64:
		return INT,vType
	case TypeString:
		return VARCHAR,vType
	case TypeBool:
		return BOOL,vType
	case TypeDecimal:
		return DECIMAL,vType
	}
	return UNDEFINED,vType
}

func (t *DbManager) ConvertString(value interface{}) (string,error) {
	data := ""
	if value == nil {
		return data,nil
	}
	convertType, vType := t.ConvertDataType(value)
	if convertType == INT {
		returnType,value,err := t.ConvertInt64OrDecimal(vType, value); if err != nil {
			return data,err
		}
		if returnType == TypeInt64 {
			data = t.Int64ToString(value.(int64))
		}else{
			data = value.(decimal.Decimal).String()
		}
	}else if convertType == DECIMAL {
		data = value.(decimal.Decimal).String()
	}else if convertType == VARCHAR {
		data = value.(string)
	}else if convertType == BOOL {
		data = t.BoolToString(value.(bool))
	}else{
		return data,fmt.Errorf("`%s` not convert string", vType)
	}
	return data,nil
}

func (t *DbManager) ConvertColumnData(column Column, value interface{}) (interface{},error) {
	var data interface{}
	var err error
	if value == nil {
		return nil,nil
	}
	columnType := column.Type
	if columnType == VARCHAR {
		data, err = t.ConvertString(value); if err != nil {
			return data, fmt.Errorf("column `%s` data `%s` convert string error %s", column.Name, value, err)
		}
	} else{
		convertType, vType := t.ConvertDataType(value)
		if columnType == BOOL && (convertType == VARCHAR || convertType == BOOL) {
			data,err = t.ConvertBool(vType, value); if err != nil {
				return data, fmt.Errorf("column `%s` data `%s` convert bool error %s", column.Name, value, err)
			}
		} else if columnType == INT && (convertType == VARCHAR || convertType == INT) {
			vType,data,err = t.ConvertInt64OrDecimal(vType, value); if vType != TypeInt64 || err != nil {
				return data, fmt.Errorf("column `%s` data `%s` convert int64 error %s", column.Name, value, err)
			}
		} else if columnType == DECIMAL && (convertType == VARCHAR || convertType == INT || convertType == DECIMAL) {
			data,err = t.ConvertDecimal(vType, value); if err != nil {
				return data, fmt.Errorf("column `%s` data `%s` convert decimal error %s", column.Name, value, err)
			}
		} else {
			return data, fmt.Errorf("column `%s` datatype `%s` error", column.Name, vType)
		}
	}
	return data,nil
}

func (t *DbManager) MatchForeignKeyByKey(foreignKeys []ForeignKey, key string) (bool,ForeignKey) {
	for _,v := range foreignKeys {
		if v.Column == key {
			return true,v
		}
	}
	return false,ForeignKey{}
}

func (t *DbManager) MatchForeignKeyByTable(foreignKeys []ForeignKey, tableName string) (bool,ForeignKey) {
	for _,v := range foreignKeys {
		if v.Reference.Table == tableName {
			return true,v
		}
	}
	return false,ForeignKey{}
}