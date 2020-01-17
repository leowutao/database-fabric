package db

import (
	"encoding/json"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

/////////////////// Table、Row、Schema Data Operation ///////////////////
const (
	TallyPrefix = "TALLY-"

	TablePrefix = "TABLE-"
	SchemaPrefix = "SCHEMA-"
	RowPrefix = "ROW-"
	RowIndexPrefix = "ROW_INDEX-"
	ForeignKeyPrefix = "FOREIGN_KEY-"

	TableCompositeKey = "NAME"
	SchemaCompositeKey = "NAME"
	RowCompositeKey = "TABLE~ID"

	//RowIndexCompositeKey = "TABLE~COLUMN~VALUE~ID"
	//ForeignKeyCompositeKey = "REFERENCE{TABLE~COLUMN}~FOREIGN_KEY{TABLE~COLUMN}"
)

func (t *DbManager) getAllTableData(stub shim.ChaincodeStubInterface) ([][]byte,error) {
	return t.getCompositeKeyDataList(stub, TablePrefix, TableCompositeKey, []string{}, []string{},0,false)
}

func (t *DbManager) getAllTableKey(stub shim.ChaincodeStubInterface) ([]string,error) {
	return t.getCompositeKeyList(stub, TablePrefix, TableCompositeKey, []string{}, []string{},0)
}

func (t *DbManager) getTableDataByFilter(stub shim.ChaincodeStubInterface, table string, filterVersion bool) ([]byte,error) {
	return t.getCompositeKeyData(stub, TablePrefix, TableCompositeKey, []string{table}, filterVersion)
}

func (t *DbManager) getTableData(stub shim.ChaincodeStubInterface, table string) ([]byte,error) {
	return t.getTableDataByFilter(stub, table,false)
}

func (t *DbManager) putTableData(stub shim.ChaincodeStubInterface, table string, value []byte) error {
	return t.putOrDelCompositeKeyData(stub, TablePrefix, TableCompositeKey, []string{table}, value, Set)
}

func (t *DbManager) delTableData(stub shim.ChaincodeStubInterface, table string) error {
	return t.putOrDelCompositeKeyData(stub, TablePrefix, TableCompositeKey, []string{table},nil, Del)
}

func (t *DbManager) getAllSchemaData(stub shim.ChaincodeStubInterface) ([][]byte,error) {
	return t.getCompositeKeyDataList(stub, SchemaPrefix, SchemaCompositeKey, []string{}, []string{},0,false)
}

func (t *DbManager) getAllSchemaKey(stub shim.ChaincodeStubInterface) ([]string,error) {
	return t.getCompositeKeyList(stub, SchemaPrefix, SchemaCompositeKey, []string{}, []string{},0)
}

func (t *DbManager) getSchemaDataByFilter(stub shim.ChaincodeStubInterface, schema string, filterVersion bool) ([]byte,error) {
	return t.getCompositeKeyData(stub, SchemaPrefix, SchemaCompositeKey, []string{schema}, filterVersion)
}

func (t *DbManager) getSchemaData(stub shim.ChaincodeStubInterface, schema string) ([]byte,error) {
	return t.getSchemaDataByFilter(stub, schema, false)
}

func (t *DbManager) putSchemaData(stub shim.ChaincodeStubInterface, schema string, value []byte) error {
	return t.putOrDelCompositeKeyData(stub, SchemaPrefix, SchemaCompositeKey, []string{schema}, value, Set)
}

func (t *DbManager) delSchemaData(stub shim.ChaincodeStubInterface, schema string) error {
	return t.putOrDelCompositeKeyData(stub, SchemaPrefix, SchemaCompositeKey, []string{schema},nil, Del)
}

func (t *DbManager) getAllRowData(stub shim.ChaincodeStubInterface, table string) ([][]byte,error) {
	return t.getCompositeKeyDataList(stub, RowPrefix, RowCompositeKey, []string{table}, []string{},0,false)
}

func (t *DbManager) getRowDataByFilter(stub shim.ChaincodeStubInterface, table string, id string, filterVersion bool) ([]byte,error) {
	return t.getCompositeKeyData(stub, RowPrefix, RowCompositeKey, []string{table, id}, filterVersion)
}

func (t *DbManager) getRowDataByVersion(stub shim.ChaincodeStubInterface, table string, id string, version []byte) ([]byte,error) {
	return t.getCompositeKeyDataByVersion(stub, RowPrefix, RowCompositeKey, []string{table, id}, version)
}

func (t *DbManager) getRowData(stub shim.ChaincodeStubInterface, table string, id string) ([]byte,error) {
	return t.getRowDataByFilter(stub, table, id,false)
}

func (t *DbManager) getRowDataByRange(stub shim.ChaincodeStubInterface, table string, id string, pageSize int32) ([][]byte,error) {
	var attributes []string
	if id != "" {
		attributes = append(attributes, id)
	}
	values,err := t.getCompositeKeyDataList(stub, RowPrefix, RowCompositeKey, []string{table}, attributes, pageSize,false)
	if err != nil {
		return values,err
	}
	return values,nil
}

func (t *DbManager) putRowData(stub shim.ChaincodeStubInterface, table string, id string, value []byte) error {
	return t.putOrDelCompositeKeyData(stub, RowPrefix, RowCompositeKey, []string{table, id}, value, Set)
}

func (t *DbManager) delRowData(stub shim.ChaincodeStubInterface, table string, id string) error {
	return t.putOrDelCompositeKeyData(stub, RowPrefix, RowCompositeKey, []string{table, id}, nil, Del)
}

/////////////////// History Operation ///////////////////

func (t *DbManager) getTableDataHistory(stub shim.ChaincodeStubInterface, table string, pageSize int32) ([][]byte,error) {
	return t.getDataHistory(stub, TablePrefix, []string{table}, pageSize)
}

func (t *DbManager) getTableDataHistoryTotal(stub shim.ChaincodeStubInterface, table string) int64 {
	tableBytes,_ := t.getTableDataByFilter(stub, table,true)
	if len(tableBytes) > 0 {
		version := HistoryVersion{}
		json.Unmarshal(tableBytes, &version)
		return version.Total
	}
	return 0
}

func (t *DbManager) getSchemaDataHistory(stub shim.ChaincodeStubInterface, schema string, pageSize int32) ([][]byte,error) {
	return t.getDataHistory(stub, SchemaPrefix, []string{schema}, pageSize)
}

func (t *DbManager) getSchemaDataHistoryTotal(stub shim.ChaincodeStubInterface, schema string) int64 {
	schemaBytes,_ := t.getSchemaDataByFilter(stub, schema,true)
	if len(schemaBytes) > 0 {
		version := HistoryVersion{}
		json.Unmarshal(schemaBytes, &version)
		return version.Total
	}
	return 0
}

func (t *DbManager) getRowDataHistory(stub shim.ChaincodeStubInterface, table string, id string, pageSize int32) ([][]byte,error) {
	return t.getDataHistory(stub, RowPrefix, []string{table, id}, pageSize)
}

func (t *DbManager) getRowDataHistoryTotal(stub shim.ChaincodeStubInterface, table string, id string) int64 {
	rowBytes,_ := t.getRowDataByFilter(stub, table, id,true)
	if len(rowBytes) > 0 {
		version := HistoryVersion{}
		json.Unmarshal(rowBytes, &version)
		return version.Total
	}
	return 0
}

/////////////////// Tally Operation ///////////////////

func (t *DbManager) getTallyData(stub shim.ChaincodeStubInterface, key string) ([]byte,error) {
	return t.getKey(stub, t.prefixAddKey(TallyPrefix, key))
}

func (t *DbManager) putTallyData(stub shim.ChaincodeStubInterface, key string, value []byte) error {
	return t.putOrDelKey(stub, t.prefixAddKey(TallyPrefix, key), value, Set)
}

/////////////////// Index Operation ///////////////////

func (t *DbManager) getAllRowIdByIndex(stub shim.ChaincodeStubInterface, table string, column string, value string) ([]string,error) {
	var ids []string
	key := t.prefixAddKey(RowIndexPrefix, t.compositeKey(table, column, value))
	values,err := t.getKey(stub, key); if err !=nil {
		return ids,err
	}
	if len(values) > 0 {
		err = json.Unmarshal(values, &ids); if err !=nil {
			return ids,err
		}
	}
	return ids,nil
}

func (t *DbManager) putRowIdIndex(stub shim.ChaincodeStubInterface, table string, column string, value string, id string) error {
	ids,err := t.getAllRowIdByIndex(stub, table, column, value); if err !=nil {
		return err
	}
	ids = append(ids, id)
	jsonBytes,err := t.ConvertJsonBytes(ids); if err !=nil {
		return err
	}
	return t.putOrDelKey(stub, t.prefixAddKey(RowIndexPrefix, t.compositeKey(table, column, value)), jsonBytes, Set)
}

func (t *DbManager) delRowIdIndex(stub shim.ChaincodeStubInterface, table string, column string, value string) error {
	return t.putOrDelKey(stub, t.prefixAddKey(RowIndexPrefix, t.compositeKey(table, column, value)),nil, Del)
}

func (t *DbManager) getRowIdByIndex(stub shim.ChaincodeStubInterface, table string, column string, value string) (string,error) {
	id := ""
	ids,err := t.getAllRowIdByIndex(stub, table, column, value); if err !=nil {
		return id,err
	}
	if ids != nil && len(ids) > 0 {
		id = ids[len(ids)-1]
	}
	return id,nil
}

/////////////////// ForeignKey Operation ///////////////////

func (t *DbManager) getAllForeignKeyByReferenceKey(stub shim.ChaincodeStubInterface, key string) ([]ReferenceKey,error) {
	var keys []ReferenceKey
	values, err := t.getKey(stub, key); if err != nil {
		return keys, err
	}
	var foreignKeys []ReferenceKey
	if len(values) > 0 {
		err = json.Unmarshal(values, &foreignKeys); if err != nil {
			return keys, err
		}
	}
	return keys,nil
}

func (t *DbManager) getAllForeignKeyByReference(stub shim.ChaincodeStubInterface, referenceTable string, referenceColumn string) ([]ReferenceForeignKey,error) {
	var keys []ReferenceForeignKey
	key := t.prefixAddKey(ForeignKeyPrefix, t.compositeKey(referenceTable, referenceColumn))
	foreignKeys,err := t.getAllForeignKeyByReferenceKey(stub, key); if err !=nil {
		return keys,err
	}
	for _,k := range foreignKeys {
		keys = append(keys, t.referenceForeignKey(referenceTable, referenceColumn, k.Table, k.Column))
	}
	return keys,nil
}

func (t *DbManager) putForeignKey(stub shim.ChaincodeStubInterface, foreignKey ReferenceForeignKey) error {
	key := t.prefixAddKey(ForeignKeyPrefix, t.compositeKey(foreignKey.Reference.Table, foreignKey.Reference.Column))
	foreignKeys,err := t.getAllForeignKeyByReferenceKey(stub, key); if err !=nil {
		return err
	}
	foreignKeys = append(foreignKeys, foreignKey.ForeignKey)
	jsonBytes,err := t.ConvertJsonBytes(foreignKey); if err !=nil {
		return err
	}
	return t.putOrDelKey(stub, key, jsonBytes, Set)
}

func (t *DbManager) delForeignKey(stub shim.ChaincodeStubInterface, foreignKey ReferenceForeignKey) error {
	key := t.prefixAddKey(ForeignKeyPrefix, t.compositeKey(foreignKey.Reference.Table, foreignKey.Reference.Column))
	return t.putOrDelKey(stub, key,nil, Del)
}

func (t *DbManager) getForeignKey(stub shim.ChaincodeStubInterface, foreignKey ReferenceForeignKey) (ReferenceForeignKey,error) {
	var key ReferenceForeignKey
	keys,err := t.getAllForeignKeyByReference(stub, foreignKey.Reference.Table, foreignKey.Reference.Column); if err !=nil {
		return key,err
	}
	if len(keys) > 0 {
		for _,k := range keys {
			if k.ForeignKey.Table == foreignKey.ForeignKey.Table && k.ForeignKey.Column == foreignKey.ForeignKey.Column {
				return k,nil
			}
		}
	}
	return key,nil
}

func (t *DbManager) getForeignKeyByReference(stub shim.ChaincodeStubInterface, referenceTable string, referenceColumn string) (ReferenceForeignKey,error) {
	var key ReferenceForeignKey
	keys,err := t.getAllForeignKeyByReference(stub, referenceTable, referenceColumn); if err !=nil {
		return key,err
	}
	if len(keys) > 0 {
		return keys[0],nil
	}
	return key,nil
}

func (t *DbManager) foreignKeyCompositeKey(foreignKey ReferenceForeignKey) []string {
	return []string{foreignKey.Reference.Table, foreignKey.Reference.Column, foreignKey.ForeignKey.Table, foreignKey.ForeignKey.Column}
}

func (t *DbManager) referenceForeignKey(keys... string) ReferenceForeignKey {
	if len(keys) >0 {
		return ReferenceForeignKey{ReferenceKey{keys[0],keys[1]},ReferenceKey{keys[2],keys[3]}}
	}
	return ReferenceForeignKey{}
}