package db

import (
	"encoding/json"
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

	BPTreeHeadPrefix = "TREE_INDEX_H-"
	BPTreeNodePrefix = "TREE_INDEX_N-"
)

func (t *DbManager) getAllTableData() ([][]byte,error) {
	return t.getCompositeKeyDataList(TablePrefix, TableCompositeKey, []string{}, []string{},0,false)
}

func (t *DbManager) getAllTableKey() ([]string,error) {
	return t.getCompositeKeyList(TablePrefix, TableCompositeKey, []string{}, []string{},0)
}

func (t *DbManager) getTableDataByFilter(table string, filterVersion bool) ([]byte,error) {
	return t.getCompositeKeyData(TablePrefix, TableCompositeKey, []string{table}, filterVersion)
}

func (t *DbManager) getTableData(table string) ([]byte,error) {
	return t.getTableDataByFilter(table,false)
}

func (t *DbManager) putTableData(table string, value []byte) error {
	return t.putOrDelCompositeKeyData(TablePrefix, TableCompositeKey, []string{table}, value, Set)
}

func (t *DbManager) delTableData(table string) error {
	return t.putOrDelCompositeKeyData(TablePrefix, TableCompositeKey, []string{table},nil, Del)
}

func (t *DbManager) getAllSchemaData() ([][]byte,error) {
	return t.getCompositeKeyDataList(SchemaPrefix, SchemaCompositeKey, []string{}, []string{},0,false)
}

func (t *DbManager) getAllSchemaKey() ([]string,error) {
	return t.getCompositeKeyList(SchemaPrefix, SchemaCompositeKey, []string{}, []string{},0)
}

func (t *DbManager) getSchemaDataByFilter(schema string, filterVersion bool) ([]byte,error) {
	return t.getCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema}, filterVersion)
}

func (t *DbManager) getSchemaData(schema string) ([]byte,error) {
	return t.getSchemaDataByFilter(schema, false)
}

func (t *DbManager) putSchemaData(schema string, value []byte) error {
	return t.putOrDelCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema}, value, Set)
}

func (t *DbManager) delSchemaData(schema string) error {
	return t.putOrDelCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema},nil, Del)
}

func (t *DbManager) getAllRowData(table string) ([][]byte,error) {
	return t.getCompositeKeyDataList(RowPrefix, RowCompositeKey, []string{table}, []string{},0,false)
}

func (t *DbManager) getRowDataByFilter(table string, id string, filterVersion bool) ([]byte,error) {
	return t.getCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id}, filterVersion)
}

func (t *DbManager) getRowDataByVersion(table string, id string, version []byte) ([]byte,error) {
	return t.getCompositeKeyDataByVersion(RowPrefix, RowCompositeKey, []string{table, id}, version)
}

func (t *DbManager) getRowData(table string, id string) ([]byte,error) {
	return t.getRowDataByFilter(table, id,false)
}

func (t *DbManager) getRowDataByRange(table string, id string, pageSize int32) ([][]byte,error) {
	var attributes []string
	if id != "" {
		attributes = append(attributes, id)
	}
	values,err := t.getCompositeKeyDataList(RowPrefix, RowCompositeKey, []string{table}, attributes, pageSize,false)
	if err != nil {
		return values,err
	}
	return values,nil
}

func (t *DbManager) putRowData(table string, id string, value []byte) error {
	return t.putOrDelCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id}, value, Set)
}

func (t *DbManager) delRowData(table string, id string) error {
	return t.putOrDelCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id}, nil, Del)
}

/////////////////// History Operation ///////////////////

func (t *DbManager) getTableDataHistory(table string, pageSize int32) ([][]byte,error) {
	return t.getDataHistory(TablePrefix, []string{table}, pageSize)
}

func (t *DbManager) getTableDataHistoryTotal(table string) int64 {
	tableBytes,_ := t.getTableDataByFilter(table,true)
	if len(tableBytes) > 0 {
		version := HistoryVersion{}
		json.Unmarshal(tableBytes, &version)
		return version.Total
	}
	return 0
}

func (t *DbManager) getSchemaDataHistory(schema string, pageSize int32) ([][]byte,error) {
	return t.getDataHistory(SchemaPrefix, []string{schema}, pageSize)
}

func (t *DbManager) getSchemaDataHistoryTotal(schema string) int64 {
	schemaBytes,_ := t.getSchemaDataByFilter(schema,true)
	if len(schemaBytes) > 0 {
		version := HistoryVersion{}
		json.Unmarshal(schemaBytes, &version)
		return version.Total
	}
	return 0
}

func (t *DbManager) getRowDataHistory(table string, id string, pageSize int32) ([][]byte,error) {
	return t.getDataHistory(RowPrefix, []string{table, id}, pageSize)
}

func (t *DbManager) getRowDataHistoryTotal(table string, id string) int64 {
	rowBytes,_ := t.getRowDataByFilter(table, id,true)
	if len(rowBytes) > 0 {
		version := HistoryVersion{}
		json.Unmarshal(rowBytes, &version)
		return version.Total
	}
	return 0
}

/////////////////// Tally Operation ///////////////////

func (t *DbManager) getTallyData(key string) ([]byte,error) {
	return t.getKey(t.prefixAddKey(TallyPrefix, key))
}

func (t *DbManager) putTallyData(key string, value []byte) error {
	return t.putOrDelKey(t.prefixAddKey(TallyPrefix, key), value, Set)
}

/////////////////// B+Tree Index Operation ///////////////////

func (t *DbManager) getBPTreeHeadPrefix(table string, column string) string {
	return t.prefixAddKey(BPTreeHeadPrefix, t.compositeKey(table, column))
}

func (t *DbManager) putBPTreeHead(table string, column string, value []byte) error {
	return t.putOrDelKey(t.getBPTreeHeadPrefix(table, column), value, Set)
}

func (t *DbManager) getBPTreeHead(table string, column string) ([]byte,error) {
	return t.getKey(t.getBPTreeHeadPrefix(table, column))
}

func (t *DbManager) getBPTreeNodePrefix(table string, column string, pointer string) string {
	return t.prefixAddKey(BPTreeNodePrefix, t.compositeKey(table, column, pointer))
}

func (t *DbManager) putBPTreeNode(table string, column string, pointer string, value []byte) error {
	return t.putOrDelKey(t.getBPTreeNodePrefix(table, column, pointer), value, Set)
}

func (t *DbManager) getBPTreeNode(table string, column string, pointer string) ([]byte,error) {
	return t.getKey(t.getBPTreeNodePrefix(table, column, pointer))
}

/////////////////// Index Operation ///////////////////

func (t *DbManager) getAllRowIdByIndex(table string, column string, value string) ([]string,error) {
	var ids []string
	key := t.prefixAddKey(RowIndexPrefix, t.compositeKey(table, column, value))
	values,err := t.getKey(key); if err !=nil {
		return ids,err
	}
	if len(values) > 0 {
		err = json.Unmarshal(values, &ids); if err !=nil {
			return ids,err
		}
	}
	return ids,nil
}

func (t *DbManager) putRowIdIndex(table string, column string, value string, id string) error {
	ids,err := t.getAllRowIdByIndex(table, column, value); if err !=nil {
		return err
	}
	ids = append(ids, id)
	jsonBytes,err := t.ConvertJsonBytes(ids); if err !=nil {
		return err
	}
	return t.putOrDelKey(t.prefixAddKey(RowIndexPrefix, t.compositeKey(table, column, value)), jsonBytes, Set)
}

func (t *DbManager) delRowIdIndex(table string, column string, value string) error {
	return t.putOrDelKey(t.prefixAddKey(RowIndexPrefix, t.compositeKey(table, column, value)),nil, Del)
}

func (t *DbManager) getRowIdByIndex(table string, column string, value string) (string,error) {
	id := ""
	ids,err := t.getAllRowIdByIndex(table, column, value); if err !=nil {
		return id,err
	}
	if ids != nil && len(ids) > 0 {
		id = ids[len(ids)-1]
	}
	return id,nil
}

/////////////////// ForeignKey Operation ///////////////////

func (t *DbManager) getAllForeignKeyByReferenceKey(key string) ([]ReferenceKey,error) {
	var keys []ReferenceKey
	values, err := t.getKey(key); if err != nil {
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

func (t *DbManager) getAllForeignKeyByReference(referenceTable string, referenceColumn string) ([]ReferenceForeignKey,error) {
	var keys []ReferenceForeignKey
	key := t.prefixAddKey(ForeignKeyPrefix, t.compositeKey(referenceTable, referenceColumn))
	foreignKeys,err := t.getAllForeignKeyByReferenceKey(key); if err !=nil {
		return keys,err
	}
	for _,k := range foreignKeys {
		keys = append(keys, t.referenceForeignKey(referenceTable, referenceColumn, k.Table, k.Column))
	}
	return keys,nil
}

func (t *DbManager) putForeignKey(foreignKey ReferenceForeignKey) error {
	key := t.prefixAddKey(ForeignKeyPrefix, t.compositeKey(foreignKey.Reference.Table, foreignKey.Reference.Column))
	foreignKeys,err := t.getAllForeignKeyByReferenceKey(key); if err !=nil {
		return err
	}
	foreignKeys = append(foreignKeys, foreignKey.ForeignKey)
	jsonBytes,err := t.ConvertJsonBytes(foreignKey); if err !=nil {
		return err
	}
	return t.putOrDelKey(key, jsonBytes, Set)
}

func (t *DbManager) delForeignKey(foreignKey ReferenceForeignKey) error {
	key := t.prefixAddKey(ForeignKeyPrefix, t.compositeKey(foreignKey.Reference.Table, foreignKey.Reference.Column))
	return t.putOrDelKey(key,nil, Del)
}

func (t *DbManager) getForeignKey(foreignKey ReferenceForeignKey) (ReferenceForeignKey,error) {
	var key ReferenceForeignKey
	keys,err := t.getAllForeignKeyByReference(foreignKey.Reference.Table, foreignKey.Reference.Column); if err !=nil {
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

func (t *DbManager) getForeignKeyByReference(referenceTable string, referenceColumn string) (ReferenceForeignKey,error) {
	var key ReferenceForeignKey
	keys,err := t.getAllForeignKeyByReference(referenceTable, referenceColumn); if err !=nil {
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