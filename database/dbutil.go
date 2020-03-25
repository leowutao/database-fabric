package database

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db/history"
	"gitee.com/bidpoc/database-fabric-cc/db/index"
	"gitee.com/bidpoc/database-fabric-cc/db/other"
	"gitee.com/bidpoc/database-fabric-cc/db/row"
	"gitee.com/bidpoc/database-fabric-cc/db/schema"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/table"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
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

	state := state.NewStateImpl(stub)//默认合约状态结构实现，如果需要自定义可以实现state.ChainCodeState接口
	return Operation(state, op, function, collection, args)
}


func Operation(state state.ChainCodeState, op string, function string, collection string, args []string) pb.Response {
	if op == "table" {
		return TableOperation(state, function, args)
	}else if op == "schema" {
		return SchemaOperation(state, function, args)
	}else if op == "tableRow" {
		return TableRowOperation(state, function, args)
	}else if op == "schemaRow" {
		return SchemaRowOperation(state, function, args)
	}else if op == "other" {
		return OtherOperation(state, collection, function, args)
	}
	return shim.Error("Invalid invoke operation name. Expecting \"table\" \"schema\" \"tableRow\" \"schemaRow\" \"other\"")
}

func TableOperation(state state.ChainCodeState, function string, args []string) pb.Response {
	stub := state.GetStub()
	if function == "add" {
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		tableJson,in := transient[TABLEJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", TABLEJSON))
		}
		name,err := table.NewTableService(state).AddTableByJson(string(tableJson), index.NewIndexService(state)); if err != nil {
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
		name,err := table.NewTableService(state).UpdateTableByJson(string(tableJson), index.NewIndexService(state)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(name))
	}else if function == "delete" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		tableName := args[0]
		err := table.NewTableService(state).DelTable(tableName, index.NewIndexService(state)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(nil)
	}else if function == "get" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		tableName := args[0]
		bytes,err := table.NewTableService(state).QueryTableBytes(tableName); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "history" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		tableName := args[0]
		number := args[1]
		pageSize,err := util.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := history.NewHistoryService(state).QueryTableHistoryBytes(tableName, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "all" {
		bytes,err := table.NewTableService(state).QueryAllTableNameBytes(); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\" \"history\" \"all\"")
}

func SchemaOperation(state state.ChainCodeState, function string, args []string) pb.Response {
	stub := state.GetStub()
	if function == "add" {
		transient,err := stub.GetTransient(); if err != nil {
			return shim.Error(err.Error())
		}
		schemaJson,in := transient[SCHEMAJSON]
		if !in {
			return shim.Error(fmt.Sprintf("GetTransient %s is null", SCHEMAJSON))
		}
		name,err := schema.NewSchemaService(state).AddSchemaByJson(string(schemaJson)); if err != nil {
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
		name,err := schema.NewSchemaService(state).UpdateSchemaByJson(string(schemaJson)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(name))
	}else if function == "delete" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		schemaName := args[0]
		if err := schema.NewSchemaService(state).DelSchema(schemaName); err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(nil)
	}else if function == "get" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		schemaName := args[0]
		bytes,err := schema.NewSchemaService(state).QuerySchemaBytes(schemaName); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "history" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		schemaName := args[0]
		number := args[1]
		pageSize,err := util.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := history.NewHistoryService(state).QuerySchemaHistoryBytes(schemaName, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "all" {
		bytes,err := schema.NewSchemaService(state).QueryAllSchemaNameBytes(); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\" \"history\" \"all\"")
}

func TableRowOperation(state state.ChainCodeState, function string, args []string) pb.Response {
	stub := state.GetStub()
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
		ids,err := row.NewRowService(state).AddRowByJson(tableName, string(rowJson), table.NewTableService(state), index.NewIndexService(state)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := util.ConvertJsonBytes(ids)
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
		ids,err := row.NewRowService(state).UpdateRowByJson(tableName, string(rowJson), table.NewTableService(state), index.NewIndexService(state)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := util.ConvertJsonBytes(ids)
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
		err := row.NewRowService(state).DelRowById(tableName, id, table.NewTableService(state), index.NewIndexService(state)); if err != nil {
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
		pageSize,err := util.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := row.NewRowService(state).QueryRowWithPaginationBytes(tableName, id, int32(pageSize), table.NewTableService(state)); if err != nil {
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
		pageSize,err := util.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := history.NewHistoryService(state).QueryRowHistoryBytes(tableName, id, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\" \"history\"")
}

func SchemaRowOperation(state state.ChainCodeState, function string, args []string) pb.Response {
	stub := state.GetStub()
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
		ids, _, err := schema.NewSchemaService(state).AddSchemaRowByJson(schemaName, string(rowJson)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := util.ConvertJsonBytes(ids)
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
		ids, _, err := schema.NewSchemaService(state).UpdateSchemaRowByJson(schemaName, id, string(rowJson)); if err != nil {
			return shim.Error(err.Error())
		}
		bytes, err := util.ConvertJsonBytes(ids)
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
		rows,err := schema.NewSchemaService(state).DelSchemaRow(schemaName, id); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := util.ConvertJsonBytes(rows)
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
		pageSize,err := util.StringToInt64(number); if err != nil {
			return shim.Error(err.Error())
		}
		bytes,err := schema.NewSchemaService(state).QuerySchemaRowByWithPaginationBytes(schemaName, id, int32(pageSize)); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"add\" \"update\" \"delete\" \"get\"")
}

func OtherOperation(state state.ChainCodeState, collection string, function string, args []string) pb.Response {
	if function == "getState" {
		if len(args) < 1 {
			return shim.Error("args length < 1")
		}
		key := args[0]
		bytes,err := other.NewOtherService(state).GetState(collection, key); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}else if function == "getStateByRange" {
		if len(args) < 2 {
			return shim.Error("args length < 2")
		}
		startKey := args[0]
		endKey := args[1]
		bytes,err := other.NewOtherService(state).GetStateByRange(collection, startKey, endKey); if err != nil {
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
		bytes,err := other.NewOtherService(state).GetStateByPartialCompositeKey(collection, objectType, keys); if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(bytes)
	}
	return shim.Error("Invalid invoke function name. Expecting \"GetState\" \"getStateByRange\" \"getStateByPartialCompositeKey\"")
}