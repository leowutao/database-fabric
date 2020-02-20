package database

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

const (
	TABLEJSON = "TABLEJSON"
	SCHEMAJSON = "SCHEMAJSON"
	ROWJSON = "ROWJSON"
)

func Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function,parameters := stub.GetFunctionAndParameters()
	fmt.Println("  ========  curr method is big invoke   ========== ")
	fmt.Printf(" function: %s ,parm is %s \n " , function ,parameters)

	if len(parameters) < 1 {
		return shim.Error("parameters `collection` is require")
	}
	if len(parameters) < 2 {
		return shim.Error("parameters `op` is require")
	}
	collection := parameters[0]
	op := parameters[1]
	if collection == "" {
		return shim.Error("collection is null")
	}
	if op == "" {
		return shim.Error("op is null")
	}

	args := parameters[2:]

	return Operation(stub, op, function, collection, args)
}


func Operation(stub shim.ChaincodeStubInterface, op string, function string, collection string, args []string) pb.Response {
	t := new(db.DbManager)
	t.ChainCodeStub = stub
	t.CacheData = map[string][]byte{}
	if op == "table" {
		return TableOperation(t, function, args)
	}else if op == "schema" {
		return SchemaOperation(t, function, args)
	}else if op == "tableRow" {
		return TableRowOperation(t, function, args)
	}else if op == "schemaRow" {
		return SchemaRowOperation(t, function, args)
	}else if op == "other" {
		return OtherOperation(t, collection, function, args)
	}
	return shim.Error("Invalid invoke operation name. Expecting \"table\" \"schema\" \"tableRow\" \"schemaRow\" \"other\"")
}

func TableOperation(t *db.DbManager, function string, args []string) pb.Response {
	stub := t.ChainCodeStub
	if function == "add" {
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		tableJson,in := transient[TABLEJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", TABLEJSON))
		}
		name,err := t.AddTableByJson(string(tableJson)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(name))
	}else if function == "update" {
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		tableJson,in := transient[TABLEJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", TABLEJSON))
		}
		name,err := t.UpdateTableByJson(string(tableJson)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(name))
	}else if function == "delete" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		tableName := args[0]
		err := t.DelTable(tableName); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(nil)
	}else if function == "get" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		tableName := args[0]
		bytes,err := t.QueryTableBytes(tableName); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "history" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		tableName := args[0]
		number := args[1]
		pageSize,err := t.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := t.QueryTableHistoryBytes(tableName, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "all" {
		bytes,err := t.QueryAllTableNameBytes(); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\" \"history\" \"all\"")
}

func SchemaOperation(t *db.DbManager, function string, args []string) pb.Response {
	stub := t.ChainCodeStub
	if function == "add" {
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		schemaJson,in := transient[SCHEMAJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", SCHEMAJSON))
		}
		name,err := t.AddSchemaByJson(string(schemaJson)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(name))
	}else if function == "update" {
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		schemaJson,in := transient[SCHEMAJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", SCHEMAJSON))
		}
		name,err := t.UpdateSchemaByJson(string(schemaJson)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(name))
	}else if function == "delete" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		schemaName := args[0]
		if err := t.DelSchema(schemaName); err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(nil)
	}else if function == "get" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		schemaName := args[0]
		bytes,err := t.QuerySchemaBytes(schemaName); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "history" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		schemaName := args[0]
		number := args[1]
		pageSize,err := t.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := t.QuerySchemaHistoryBytes(schemaName, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "all" {
		bytes,err := t.QueryAllSchemaNameBytes(); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\" \"history\" \"all\"")
}

func TableRowOperation(t *db.DbManager, function string, args []string) pb.Response {
	stub := t.ChainCodeStub
	if function == "add" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		tableName := args[0]
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		rowJson,in := transient[ROWJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", ROWJSON))
		}
		ids,err := t.AddRowByJson(tableName, string(rowJson)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := t.ConvertJsonBytes(ids)
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "update" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		tableName := args[0]
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		rowJson,in := transient[ROWJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", ROWJSON))
		}
		ids,err := t.UpdateRowByJson(tableName, string(rowJson)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := t.ConvertJsonBytes(ids)
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "delete" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		tableName := args[0]
		id := args[1]
		err := t.DelRowById(tableName, id); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(nil)
	}else if function == "get" {
		if len(args) < 3 {
			return shim.Error("args length < 3")
		}
		tableName := args[0]
		id := args[1]
		number := args[2]
		pageSize,err := t.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := t.QueryRowWithPaginationBytes(tableName, id, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "history" {
		if len(args) < 3 {
			return shim.Error("args length < 3")
		}
		tableName := args[0]
		id := args[1]
		number := args[2]
		pageSize,err := t.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := t.QueryRowHistoryBytes(tableName, id, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\" \"history\"")
}

func SchemaRowOperation(t *db.DbManager, function string, args []string) pb.Response {
	stub := t.ChainCodeStub
	if function == "add" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		schemaName := args[0]
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		rowJson,in := transient[ROWJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", ROWJSON))
		}
		ids, _, err := t.AddSchemaRowByJson(schemaName, string(rowJson)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := t.ConvertJsonBytes(ids)
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "update" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		schemaName := args[0]
		id := args[1]
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		rowJson,in := transient[ROWJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", ROWJSON))
		}
		ids, _, err := t.UpdateSchemaRowByJson(schemaName, id, string(rowJson)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := t.ConvertJsonBytes(ids)
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "delete" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		schemaName := args[0]
		id := args[1]
		rows,err := t.DelSchemaRow(schemaName, id); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := t.ConvertJsonBytes(rows)
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "get" {
		if len(args) < 3 {
			return shim.Error("args length < 3")
		}
		schemaName := args[0]
		id := args[1]
		number := args[2]
		pageSize,err := t.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := t.QuerySchemaRowByWithPaginationBytes(schemaName, id, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\"")
}

func OtherOperation(t *db.DbManager, collection string, function string, args []string) pb.Response {
	if function == "getState" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		key := args[0]
		bytes,err := t.GetState(collection, key); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "getStateByRange" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		startKey := args[0]
		endKey := args[1]
		bytes,err := t.GetStateByRange(collection, startKey, endKey); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "getStateByPartialCompositeKey" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		objectType := args[0]
		var keys []string
		if len(args) > 1 {
			keys = args[1:]
		}
		bytes,err := t.GetStateByPartialCompositeKey(collection, objectType, keys); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"GetState\" \"getStateByRange\" \"getStateByPartialCompositeKey\"")
}