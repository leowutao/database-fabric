package storage

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)
////////////////////////////////////// Common Storage //////////////////////////////////////

type CommonStorage struct {
	state state.ChainCodeState
}

func (storage *CommonStorage) Init(state state.ChainCodeState)  {
	storage.state = state
}

func (storage *CommonStorage) GetTxID() (string,int64,error) {
	txID := storage.state.GetStub().GetTxID()
	timestamp,err := storage.state.GetStub().GetTxTimestamp(); if err != nil {
		return txID,0,err
	}
	return txID,timestamp.Seconds,nil
}

func (storage *CommonStorage) getNames(key string) ([]string,error) {
	bytes,err := storage.state.GetKey(key); if err != nil {
		return nil,err
	}
	var names []string
	if len(bytes) > 0 {
		err := json.Unmarshal(bytes, &names); if err != nil {
			return nil,err
		}
	}
	return names,nil
}

func (storage *CommonStorage) putNames(key string, names []string) error {
	bytes,err := util.ConvertJsonBytes(names); if err !=nil {
		return err
	}
	return storage.state.PutOrDelKey(key, bytes, db.SetState)
}

func (storage *CommonStorage) addName(key string, name string) (int,error) {
	names,err := storage.getNames(key); if err != nil {
		return 0,err
	}
	length := len(names)
	length++
	names = append(names, name)
	bytes,err := util.ConvertJsonBytes(names); if err !=nil {
		return 0,err
	}
	return length,storage.state.PutOrDelKey(key, bytes, db.SetState)
}

func (storage *CommonStorage) findName(key string, name string) (int,error) {
	names,err := storage.getNames(key); if err != nil {
		return 0,err
	}
	for i,v := range names {
		if name == v {
			return i+1,nil
		}
	}
	return 0,nil
}

func (storage *CommonStorage) getChainDataKey() string {
	return storage.state.PrefixAddKey(db.ChainPrefix, string(db.ChainKeyType))
}

func (storage *CommonStorage) getDataBaseDataKey(database db.DataBaseID) string {
	return storage.state.PrefixAddKey(string(db.DataBaseKeyType), string(database))
}

func (storage *CommonStorage) getRelationDataKey(database db.DataBaseID) string {
	return storage.state.PrefixAddKey(string(db.RelationKeyType), string(database))
}

func (storage *CommonStorage) getTableDataKey(database db.DataBaseID, table db.TableID) string {
	return storage.state.PrefixAddKey(string(db.TableKeyType), storage.state.CompositeKey(string(database), string(table)))
}

func (storage *CommonStorage) getBlockDataKey(database db.DataBaseID, table db.TableID, block db.BlockID) string {
	return storage.state.PrefixAddKey(string(db.BlockKeyType), storage.state.CompositeKey(string(database), string(table), string(block)))
}

func (storage *TableStorage) createDataBase(name string) (db.DataBaseID,error) {
	id,err := storage.addName(storage.getChainDataKey(), name)
	return db.DataBaseID(id),err
}

func (storage *CommonStorage) getDataBase(name string) (db.DataBaseID,error) {
	id,err := storage.findName(storage.getChainDataKey(), name)
	return db.DataBaseID(id),err
}

func (storage *CommonStorage) getAllTable(database db.DataBaseID) ([]string,error) {
	return storage.getNames(storage.getDataBaseDataKey(database))
}

func (storage *CommonStorage) createTable(database db.DataBaseID, name string) (db.TableID,error) {
	id,err := storage.addName(storage.getDataBaseDataKey(database), name)
	return db.TableID(id),err
}

func (storage *CommonStorage) updateTable(database db.DataBaseID, table db.TableID, name string) error {
	key := storage.getDataBaseDataKey(database)
	names,err := storage.getNames(key); if err != nil {
		return err
	}
	if db.TableID(len(names)) < table {
		return fmt.Errorf("table id not found")
	}
	names[table-1] = name
	return storage.putNames(key, names)
}

func (storage *CommonStorage) deleteTable(database db.DataBaseID, table db.TableID) error {
	return storage.updateTable(database, table,"")
}

func (storage *CommonStorage) getTable(database db.DataBaseID, name string) (db.TableID,error) {
	id,err := storage.findName(storage.getDataBaseDataKey(database), name)
	return db.TableID(id),err
}

func (storage *CommonStorage) getTableName(database db.DataBaseID, table db.TableID) (string,error) {
	key := storage.getDataBaseDataKey(database)
	names,err := storage.getNames(key); if err != nil {
		return "",err
	}
	if db.TableID(len(names)) < table {
		return "",fmt.Errorf("table id not found")
	}
	return names[table-1],nil
}

////////////////////////////////////// Database Storage //////////////////////////////////////
type DatabaseStorage struct {
	CommonStorage
}

func NewDatabaseStorage(state state.ChainCodeState) *DatabaseStorage {
	storage := new(DatabaseStorage)
	storage.Init(state)
	return storage
}

func (storage *DatabaseStorage) GetRelationData(database db.DataBaseID) ([]byte,error) {
	return storage.state.GetKey(storage.getRelationDataKey(database))
}

func (storage *DatabaseStorage) PutRelationData(database db.DataBaseID, value []byte) error {
	return storage.state.PutOrDelKey(storage.getRelationDataKey(database), value, db.SetState)
}

func (storage *DatabaseStorage) CreateTable(database db.DataBaseID, tableName string) (db.TableID,error) {
	return storage.createTable(database, tableName)
}

func (storage *DatabaseStorage) UpdateTable(database db.DataBaseID, tableID db.TableID, name string) error {
	return storage.updateTable(database, tableID, name)
}

func (storage *DatabaseStorage) DeleteTable(database db.DataBaseID, tableID db.TableID) error {
	return storage.deleteTable(database, tableID)
}

func (storage *DatabaseStorage) GetTable(database db.DataBaseID, name string) (db.TableID,error) {
	return storage.getTable(database, name)
}

func (storage *DatabaseStorage) GetTableName(database db.DataBaseID, tableID db.TableID) (string,error) {
	return storage.getTableName(database, tableID)
}

func (storage *DatabaseStorage) GetAllTable(database db.DataBaseID) ([]string,error) {
	return storage.getAllTable(database)
}

////////////////////////////////////// Table Storage //////////////////////////////////////
type TableStorage struct {
	CommonStorage
}

func NewTableStorage(state state.ChainCodeState) *TableStorage {
	storage := new(TableStorage)
	storage.Init(state)
	return storage
}

func (storage *TableStorage) GetTableData(database db.DataBaseID, table db.TableID) ([]byte,error) {
	return storage.state.GetKey(storage.getTableDataKey(database, table))
}

func (storage *TableStorage) PutTableData(database db.DataBaseID, table db.TableID, value []byte) error {
	return storage.state.PutOrDelKey(storage.getTableDataKey(database, table), value, db.SetState)
}



////////////////////////////////////// Block Storage //////////////////////////////////////
type BlockStorage struct {
	CommonStorage
}

func NewBlockStorage(state state.ChainCodeState) *BlockStorage {
	storage := new(BlockStorage)
	storage.Init(state)
	return storage
}

func (storage *BlockStorage) GetBlockData(database db.DataBaseID, table db.TableID, block db.BlockID) ([]byte,error) {
	return storage.state.GetKey(storage.getBlockDataKey(database, table, block))
}

func (storage *BlockStorage) PutBlockData(database db.DataBaseID, table db.TableID, block db.BlockID, value []byte) error {
	return storage.state.PutOrDelKey(storage.getBlockDataKey(database, table, block), value, db.SetState)
}

////////////////////////////////////// BPTree Storage //////////////////////////////////////

type BPTreeStorage struct {
	CommonStorage
}

func NewBPTreeStorage(state state.ChainCodeState) *BPTreeStorage {
	storage := new(BPTreeStorage)
	storage.Init(state)
	return storage
}

type BPTreeNodeType int8
const (
	HeadNodeType BPTreeNodeType = iota
	NodeType
)

func (storage *BPTreeStorage) GetHeadPrefix(key db.ColumnKey) string {
	return storage.state.PrefixAddKey(string(db.IndexKeyType), storage.state.CompositeKey(string(HeadNodeType), string(key.Database), string(key.Table), string(key.Column)))
}

func (storage *BPTreeStorage) PutHead(key db.ColumnKey, value []byte) error {
	return storage.state.PutOrDelKey(storage.GetHeadPrefix(key), value, db.SetState)
}

func (storage *BPTreeStorage) GetHead(key db.ColumnKey) ([]byte,error) {
	return storage.state.GetKey(storage.GetHeadPrefix(key))
}

func (storage *BPTreeStorage) GetNodePrefix(key db.ColumnKey, pointer string) string {
	return storage.state.PrefixAddKey(string(db.IndexKeyType), storage.state.CompositeKey(string(NodeType),  string(key.Database), string(key.Table), string(key.Column), pointer))
}

func (storage *BPTreeStorage) PutNode(key db.ColumnKey, pointer string, value []byte) error {
	return storage.state.PutOrDelKey(storage.GetNodePrefix(key, pointer), value, db.SetState)
}

func (storage *BPTreeStorage) GetNode(key db.ColumnKey, pointer string) ([]byte,error) {
	return storage.state.GetKey(storage.GetNodePrefix(key, pointer))
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


/////////////////// Other Operation ///////////////////
func (storage *OtherStorage) GetOtherState(collection, key string) ([]byte,error) {
	return storage.state.GetState(collection, key)
}

func (storage *OtherStorage) GetOtherStateByRange(collection string,  startKey string, endKey string) ([]byte,error) {
	return storage.state.GetStateByRange(collection, startKey, endKey)
}

func (storage *OtherStorage) GetOtherStateByPartialCompositeKey(collection string,  objectType string, keys []string) ([]byte,error) {
	return storage.state.GetStateByPartialCompositeKey(collection, objectType, keys)
}