package storage

import (
	"encoding/json"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type CommonStorage struct {
	state.ChainCodeState
}

func (common *CommonStorage) Init(state state.ChainCodeState)  {
	common.ChainCodeState = state
}

type BPTreeStorage struct {
	CommonStorage
}

func NewBPTreeStorage(state state.ChainCodeState) *BPTreeStorage {
	storage := new(BPTreeStorage)
	storage.Init(state)
	return storage
}

type HistoryStorage struct {
	CommonStorage
}

func NewHistoryStorage(state state.ChainCodeState) *HistoryStorage {
	storage := new(HistoryStorage)
	storage.Init(state)
	return storage
}

type RowStorage struct {
	CommonStorage
}

func NewRowStorage(state state.ChainCodeState) *RowStorage {
	storage := new(RowStorage)
	storage.Init(state)
	return storage
}

type TableStorage struct {
	CommonStorage
}

func NewTableStorage(state state.ChainCodeState) *TableStorage {
	storage := new(TableStorage)
	storage.Init(state)
	return storage
}

type SchemaStorage struct {
	CommonStorage
}

func NewSchemaStorage(state state.ChainCodeState) *SchemaStorage {
	storage := new(SchemaStorage)
	storage.Init(state)
	return storage
}

type IndexStorage struct {
	CommonStorage
}

func NewIndexStorage(state state.ChainCodeState) *IndexStorage {
	storage := new(IndexStorage)
	storage.Init(state)
	return storage
}

type OtherStorage struct {
	CommonStorage
}

func NewOtherStorage(state state.ChainCodeState) *OtherStorage {
	storage := new(OtherStorage)
	storage.Init(state)
	return storage
}

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

func (storage *TableStorage) GetAllTableData() ([][]byte,error) {
	return storage.GetCompositeKeyDataList(TablePrefix, TableCompositeKey, []string{}, []string{},0,false)
}

func (storage *TableStorage) GetAllTableKey() ([]string,error) {
	return storage.GetCompositeKeyList(TablePrefix, TableCompositeKey, []string{}, []string{},0)
}

func (storage *TableStorage) GetTableDataByFilter(table string, filterVersion bool) ([]byte,error) {
	return storage.GetCompositeKeyData(TablePrefix, TableCompositeKey, []string{table}, filterVersion)
}

func (storage *TableStorage) GetTableData(table string) ([]byte,error) {
	return storage.GetTableDataByFilter(table,false)
}

func (storage *TableStorage) PutTableData(table string, value []byte) error {
	return storage.PutOrDelCompositeKeyData(TablePrefix, TableCompositeKey, []string{table}, value, state.Set)
}

func (storage *TableStorage) DelTableData(table string) error {
	return storage.PutOrDelCompositeKeyData(TablePrefix, TableCompositeKey, []string{table},nil, state.Del)
}

func (storage *TableStorage) GetTableTallyData(table string) ([]byte,error) {
	return storage.GetKey(storage.PrefixAddKey(TallyPrefix, table))
}

func (storage *TableStorage) PutTableTallyData(table string, value []byte) error {
	return storage.PutOrDelKey(storage.PrefixAddKey(TallyPrefix, table), value, int8(state.Set))
}

func (storage *SchemaStorage) GetAllSchemaData() ([][]byte,error) {
	return storage.GetCompositeKeyDataList(SchemaPrefix, SchemaCompositeKey, []string{}, []string{},0,false)
}

func (storage *SchemaStorage) GetAllSchemaKey() ([]string,error) {
	return storage.GetCompositeKeyList(SchemaPrefix, SchemaCompositeKey, []string{}, []string{},0)
}

func (storage *SchemaStorage) GetSchemaDataByFilter(schema string, filterVersion bool) ([]byte,error) {
	return storage.GetCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema}, filterVersion)
}

func (storage *SchemaStorage) GetSchemaData(schema string) ([]byte,error) {
	return storage.GetSchemaDataByFilter(schema, false)
}

func (storage *SchemaStorage) PutSchemaData(schema string, value []byte) error {
	return storage.PutOrDelCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema}, value, state.Set)
}

func (storage *SchemaStorage) DelSchemaData(schema string) error {
	return storage.PutOrDelCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema},nil, state.Del)
}

func (storage *RowStorage) GetAllRowData(table string) ([][]byte,error) {
	return storage.GetCompositeKeyDataList(RowPrefix, RowCompositeKey, []string{table}, []string{},0,false)
}

func (storage *RowStorage) GetRowDataByFilter(table string, id string, filterVersion bool) ([]byte,error) {
	return storage.GetCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id}, filterVersion)
}

func (storage *RowStorage) GetRowDataByVersion(table string, id string, version []byte) ([]byte,error) {
	return storage.GetCompositeKeyDataByVersion(RowPrefix, RowCompositeKey, []string{table, id}, version)
}

func (storage *RowStorage) GetRowData(table string, id string) ([]byte,error) {
	return storage.GetRowDataByFilter(table, id,false)
}

func (storage *RowStorage) GetRowDataByRange(table string, id string, pageSize int32) ([][]byte,error) {
	var attributes []string
	if id != "" {
		attributes = append(attributes, id)
	}
	values,err := storage.GetCompositeKeyDataList(RowPrefix, RowCompositeKey, []string{table}, attributes, pageSize,false)
	if err != nil {
		return values,err
	}
	return values,nil
}

func (storage *RowStorage) PutRowData(table string, id string, value []byte) error {
	return storage.PutOrDelCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id}, value, state.Set)
}

func (storage *RowStorage) DelRowData(table string, id string) error {
	return storage.PutOrDelCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id}, nil, state.Del)
}

/////////////////// History Operation ///////////////////

func (storage *HistoryStorage) GetTableDataHistory(table string, pageSize int32) ([][]byte,error) {
	return storage.GetDataHistory(TablePrefix, []string{table}, pageSize)
}

func (storage *HistoryStorage) GetTableDataHistoryTotal(table string) int64 {
	tableBytes,_ := storage.GetCompositeKeyData(TablePrefix, TableCompositeKey, []string{table},true)
	if len(tableBytes) > 0 {
		version := state.HistoryVersion{}
		json.Unmarshal(tableBytes, &version)
		return version.Total
	}
	return 0
}

func (storage *HistoryStorage) GetSchemaDataHistory(schema string, pageSize int32) ([][]byte,error) {
	return storage.GetDataHistory(SchemaPrefix, []string{schema}, pageSize)
}

func (storage *HistoryStorage) GetSchemaDataHistoryTotal(schema string) int64 {
	schemaBytes,_ := storage.GetCompositeKeyData(SchemaPrefix, SchemaCompositeKey, []string{schema},true)
	if len(schemaBytes) > 0 {
		version := state.HistoryVersion{}
		json.Unmarshal(schemaBytes, &version)
		return version.Total
	}
	return 0
}

func (storage *HistoryStorage) GetRowDataHistory(table string, id string, pageSize int32) ([][]byte,error) {
	return storage.GetDataHistory(RowPrefix, []string{table, id}, pageSize)
}

func (storage *HistoryStorage) GetRowDataHistoryTotal(table string, id string) int64 {
	rowBytes,_ := storage.GetCompositeKeyData(RowPrefix, RowCompositeKey, []string{table, id},true)
	if len(rowBytes) > 0 {
		version := state.HistoryVersion{}
		json.Unmarshal(rowBytes, &version)
		return version.Total
	}
	return 0
}

/////////////////// B+Tree Index Operation ///////////////////

func (storage *BPTreeStorage) GetHeadPrefix(table string, column string) string {
	return storage.PrefixAddKey(BPTreeHeadPrefix, storage.CompositeKey(table, column))
}

func (storage *BPTreeStorage) PutHead(table string, column string, value []byte) error {
	return storage.PutOrDelKey(storage.GetHeadPrefix(table, column), value, int8(state.Set))
}

func (storage *BPTreeStorage) GetHead(table string, column string) ([]byte,error) {
	return storage.GetKey(storage.GetHeadPrefix(table, column))
}

func (storage *BPTreeStorage) GetNodePrefix(table string, column string, pointer string) string {
	return storage.PrefixAddKey(BPTreeNodePrefix, storage.CompositeKey(table, column, pointer))
}

func (storage *BPTreeStorage) PutNode(table string, column string, pointer string, value []byte) error {
	return storage.PutOrDelKey(storage.GetNodePrefix(table, column, pointer), value, int8(state.Set))
}

func (storage *BPTreeStorage) GetNode(table string, column string, pointer string) ([]byte,error) {
	return storage.GetKey(storage.GetNodePrefix(table, column, pointer))
}

/////////////////// Index Operation ///////////////////

func (storage *IndexStorage) GetAllRowIdByIndex(table string, column string, value string) ([]string,error) {
	var ids []string
	key := storage.PrefixAddKey(RowIndexPrefix, storage.CompositeKey(table, column, value))
	values,err := storage.GetKey(key); if err !=nil {
		return ids,err
	}
	if len(values) > 0 {
		err = json.Unmarshal(values, &ids); if err !=nil {
			return ids,err
		}
	}
	return ids,nil
}

func (storage *IndexStorage) PutRowIdIndex(table string, column string, value string, id string) error {
	ids,err := storage.GetAllRowIdByIndex(table, column, value); if err !=nil {
		return err
	}
	ids = append(ids, id)
	jsonBytes,err := util.ConvertJsonBytes(ids); if err !=nil {
		return err
	}
	return storage.PutOrDelKey(storage.PrefixAddKey(RowIndexPrefix, storage.CompositeKey(table, column, value)), jsonBytes, int8(state.Set))
}

func (storage *IndexStorage) DelRowIdIndex(table string, column string, value string) error {
	return storage.PutOrDelKey(storage.PrefixAddKey(RowIndexPrefix, storage.CompositeKey(table, column, value)),nil, int8(state.Del))
}

func (storage *IndexStorage) GetRowIdByIndex(table string, column string, value string) (string,error) {
	id := ""
	ids,err := storage.GetAllRowIdByIndex(table, column, value); if err !=nil {
		return id,err
	}
	if ids != nil && len(ids) > 0 {
		id = ids[len(ids)-1]
	}
	return id,nil
}

/////////////////// ForeignKey Operation ///////////////////

func (storage *IndexStorage) GetAllForeignKeyByReferenceKey(key string) ([]db.ReferenceKey,error) {
	var keys []db.ReferenceKey
	values, err := storage.GetKey(key); if err != nil {
		return keys, err
	}
	var foreignKeys []db.ReferenceKey
	if len(values) > 0 {
		err = json.Unmarshal(values, &foreignKeys); if err != nil {
			return keys, err
		}
	}
	return keys,nil
}

func (storage *IndexStorage) GetAllForeignKeyByReference(referenceTable string, referenceColumn string) ([]db.ReferenceForeignKey,error) {
	var keys []db.ReferenceForeignKey
	key := storage.PrefixAddKey(ForeignKeyPrefix, storage.CompositeKey(referenceTable, referenceColumn))
	foreignKeys,err := storage.GetAllForeignKeyByReferenceKey(key); if err !=nil {
		return keys,err
	}
	for _,k := range foreignKeys {
		keys = append(keys, storage.referenceForeignKey(referenceTable, referenceColumn, k.Table, k.Column))
	}
	return keys,nil
}

func (storage *IndexStorage) PutForeignKey(foreignKey db.ReferenceForeignKey) error {
	key := storage.PrefixAddKey(ForeignKeyPrefix, storage.CompositeKey(foreignKey.Reference.Table, foreignKey.Reference.Column))
	foreignKeys,err := storage.GetAllForeignKeyByReferenceKey(key); if err !=nil {
		return err
	}
	foreignKeys = append(foreignKeys, foreignKey.ForeignKey)
	jsonBytes,err := util.ConvertJsonBytes(foreignKey); if err !=nil {
		return err
	}
	return storage.PutOrDelKey(key, jsonBytes, int8(state.Set))
}

func (storage *IndexStorage) DelForeignKey(foreignKey db.ReferenceForeignKey) error {
	key := storage.PrefixAddKey(ForeignKeyPrefix, storage.CompositeKey(foreignKey.Reference.Table, foreignKey.Reference.Column))
	return storage.PutOrDelKey(key,nil, int8(state.Del))
}

func (storage *IndexStorage) GetForeignKey(foreignKey db.ReferenceForeignKey) (db.ReferenceForeignKey,error) {
	var key db.ReferenceForeignKey
	keys,err := storage.GetAllForeignKeyByReference(foreignKey.Reference.Table, foreignKey.Reference.Column); if err !=nil {
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

func (storage *IndexStorage) GetForeignKeyByReference(referenceTable string, referenceColumn string) (db.ReferenceForeignKey,error) {
	var key db.ReferenceForeignKey
	keys,err := storage.GetAllForeignKeyByReference(referenceTable, referenceColumn); if err !=nil {
		return key,err
	}
	if len(keys) > 0 {
		return keys[0],nil
	}
	return key,nil
}

func (storage *IndexStorage) foreignKeyCompositeKey(foreignKey db.ReferenceForeignKey) []string {
	return []string{foreignKey.Reference.Table, foreignKey.Reference.Column, foreignKey.ForeignKey.Table, foreignKey.ForeignKey.Column}
}

func (storage *IndexStorage) referenceForeignKey(keys... string) db.ReferenceForeignKey {
	if len(keys) >0 {
		return db.ReferenceForeignKey{db.ReferenceKey{keys[0],keys[1]}, db.ReferenceKey{keys[2],keys[3]}}
	}
	return db.ReferenceForeignKey{}
}

/////////////////// Other Operation ///////////////////
func (storage *OtherStorage) GetOtherState(collection, key string) ([]byte,error) {
	return storage.GetState(collection, key)
}

func (storage *OtherStorage) GetOtherStateByRange(collection string,  startKey string, endKey string) ([]byte,error) {
	return storage.GetStateByRange(collection, startKey, endKey)
}

func (storage *OtherStorage) GetOtherStateByPartialCompositeKey(collection string,  objectType string, keys []string) ([]byte,error) {
	return storage.GetStateByPartialCompositeKey(collection, objectType, keys)
}