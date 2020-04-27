package index

import (
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index/linkedlist"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree/bptree"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type IndexService struct {
	storage *storage.IndexStorage
	primaryInsert *PrimaryInsert
	iInsert *tree.InsertInterface
	iTree tree.TreeInterface
	treeHeadMap map[string]*tree.TreeHead
	iLinked linkedlist.LinkedListInterface
	linkedHeadMap map[string]*linkedlist.LinkedHead
}

func NewIndexService(state state.ChainCodeState) *IndexService {
	var iInsert *tree.InsertInterface
	*iInsert = nil
	var iTree tree.TreeInterface
	iTree = bptree.NewBPTreeImpl(storage.NewBPTreeStorage(state), tree.NewDefaultValue(iInsert))
	treeHeadMap := map[string]*tree.TreeHead{}
	var iLinked linkedlist.LinkedListInterface
	iLinked = linkedlist.NewLinkedListImpl(storage.NewLinkedListStorage(state))
	linkedHeadMap := map[string]*linkedlist.LinkedHead{}
	return &IndexService{storage.NewIndexStorage(state),NewPrimaryInsertImpl(),iInsert,iTree,treeHeadMap,iLinked,linkedHeadMap}
}

///////////////////// Tree Function //////////////////////

func (service *IndexService) getITree(primary bool) tree.TreeInterface {
	if primary {
		*service.iInsert = service.primaryInsert
	}else{
		*service.iInsert = &service.primaryInsert.DefaultInsert
	}
	return service.iTree
}

func (service *IndexService) getTreeHead(key db.ColumnKey) (*tree.TreeHead,error) {
	var err error
	name := string(key.Database)+"~"+string(key.Table)+"~"+string(key.Column)
	treeHead, ok := service.treeHeadMap[name]
	if ok {
		return treeHead,nil
	}
	treeHead,err = service.iTree.CreateHead(key, tree.TreeTypeDefault); if err != nil {
		return nil,err
	}
	service.treeHeadMap[name] = treeHead
	return treeHead,nil
}

///////////////////// Linked Function //////////////////////

func (service *IndexService) getILinked() linkedlist.LinkedListInterface {
	return service.iLinked
}

func (service *IndexService) getLinkedHead(columnKey db.ColumnKey, rowID db.RowID) (*linkedlist.LinkedHead,error) {
	var err error
	key := db.ColumnRowKey{ColumnKey:columnKey,Row:rowID}
	name := string(key.Database)+"~"+string(key.Table)+"~"+string(key.Column)+"~"+string(key.Row)
	linkedHead, ok := service.linkedHeadMap[name]
	if ok {
		return linkedHead,nil
	}
	linkedHead,err = service.iLinked.CreateHead(key); if err != nil {
		return nil,err
	}
	service.linkedHeadMap[name] = linkedHead
	return linkedHead,nil
}

///////////////////// Common IndexData Function //////////////////////

func (service *IndexService) putIndexData(columnKey db.ColumnKey, key []byte, value []byte, insertType tree.InsertType, primary bool) error {
	treeHead,err := service.getTreeHead(columnKey); if err != nil {
		return err
	}
	refNode,err := service.getITree(primary).Insert(treeHead, key, value, insertType); if err != nil {
		return err
	}
	if refNode.Kv.VType == db.ValueTypeLinkedList {
		linkedHead,err := service.getLinkedHead(columnKey, util.BytesToRowID(refNode.Kv.Key)); if err != nil {
			return err
		}
		return service.getILinked().Insert(linkedHead, refNode.Values)
	}
	return nil
}

func (service *IndexService) getIndexDataValues(columnKey db.ColumnKey, kv *db.KV, order db.OrderType, size int32, primary bool) ([][]byte,db.Total,error) {
	isLinked := (!primary || size > 1) && kv.VType == db.ValueTypeLinkedList
	if  isLinked {
		linkedHead,err := service.getLinkedHead(columnKey, util.BytesToRowID(kv.Key)); if err != nil {
			return nil,0,err
		}
		return service.getILinked().SearchByRange(linkedHead, order, size)
	}else if primary || kv.VType == db.ValueTypeCollection {
		values,err := service.primaryInsert.parse.CollectionBytes(kv.Value); if err != nil {
			return nil,0,err
		}
		total := db.Total(len(values))
		if total > 1 && order == db.DESC {//数组反转
			values = service.primaryInsert.parse.CollectionFlip(values)
		}
		return values[:size],total,nil
	}else{
		return [][]byte{kv.Value},1,nil
	}
}

func (service *IndexService) getIndexData(columnKey db.ColumnKey, key []byte, order db.OrderType, size int32, primary bool) ([][]byte,db.Total,error) {
	treeHead,err := service.getTreeHead(columnKey); if err != nil {
		return nil,0,err
	}
	kv,err := service.getITree(primary).Search(treeHead, key); if err != nil {
		return nil,0,err
	}
	return service.getIndexDataValues(columnKey, kv, order, size, primary)
}

func (service *IndexService) getIndexDataByRange(columnKey db.ColumnKey, start []byte, end []byte, order db.OrderType, size int32, primary bool) ([]*db.KV,error) {
	treeHead,err := service.getTreeHead(columnKey); if err != nil {
		return nil,err
	}
	kvList,err := service.getITree(primary).SearchByRange(treeHead, start, end, order, size); if err != nil {
		return nil,err
	}
	valueOrder := db.ASC
	if primary {
		valueOrder = db.DESC
	}
	for _,kv := range kvList {
		values,_,err := service.getIndexDataValues(columnKey, kv, valueOrder,1, primary); if err != nil {
			return nil,err
		}
		if len(values) > 0 {
			kv.Value = values[0]
		}else{
			kv.Value = nil
		}
	}
	return kvList,nil
}

///////////////////// PrimaryKey Index Function //////////////////////

func (service *IndexService) PutPrimaryKeyIndex(database db.DatabaseID, table *db.TableData, rowID db.RowID, op db.OpType, blockID db.BlockID) error {
	columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:table.PrimaryKey.ColumnID}
	return service.putIndexData(columnKey, util.RowIDToBytes(rowID), service.primaryInsert.parse.FormatBlockType(blockID, op), tree.InsertTypeAppend,true)
}

func (service *IndexService) GetPrimaryKeyIndex(database db.DatabaseID, table *db.TableData, rowID db.RowID) (db.BlockID,error) {
	columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:table.PrimaryKey.ColumnID}
	values,_,err := service.getIndexData(columnKey, util.RowIDToBytes(rowID), db.DESC,1,true); if err != nil {
		return 0,err
	}
	if len(values) > 0 {
		return service.primaryInsert.parse.BlockID(values[0])
	}
	return 0,nil
}

func (service *IndexService) GetPrimaryKeyIndexByRange(database db.DatabaseID, table *db.TableData, start db.RowID, end db.RowID, order db.OrderType, size int32) (db.RowBlockID,error) {
	columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:table.PrimaryKey.ColumnID}
	kvList,err := service.getIndexDataByRange(columnKey, util.RowIDToBytes(start), util.RowIDToBytes(end), order, size,true); if err != nil {
		return nil,err
	}
	return service.primaryInsert.parse.RowBlockID(kvList)
}

func (service *IndexService) GetPrimaryKeyIndexHistoryByRange(database db.DatabaseID, table *db.TableData, rowID db.RowID, order db.OrderType, size int32) ([]db.BlockID,db.Total,error) {
	columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:table.PrimaryKey.ColumnID}
	values,total,err := service.getIndexData(columnKey, util.RowIDToBytes(rowID), order, size,true); if err != nil {
		return nil,0,err
	}
	blocks,err := service.primaryInsert.parse.BlockIDList(values); if err != nil {
		return nil,0,err
	}
	return blocks,total,nil
}

///////////////////// ForeignKey Index Function //////////////////////

func (service *IndexService) PutForeignKeysIndex(database db.DatabaseID, table *db.TableData, rowID db.RowID, row *db.RowData) error {
	for _,foreignKey := range table.ForeignKeys {
		value := row.Data[foreignKey.ColumnID-1]
		if len(value) > 0 {
			columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:foreignKey.ColumnID}
			if err := service.putIndexData(columnKey, value, util.RowIDToBytes(rowID), tree.InsertTypeAppend,false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (service *IndexService) GetForeignKeyIndex(database db.DatabaseID, tableID db.TableID, foreignKey db.ForeignKey, referenceRowID db.RowID, size int32) ([]db.RowID,error) {
	columnKey := db.ColumnKey{Database:database,Table:tableID,Column:foreignKey.ColumnID}
	values,_,err := service.getIndexData(columnKey, util.RowIDToBytes(referenceRowID), db.ASC, size,false); if err != nil {
		return nil,err
	}
	return service.primaryInsert.parse.RowIDList(values)
}

///////////////////// Other Index Function //////////////////////

func (service *IndexService) QueryRowIdByIndex(key db.ColumnKey, value []byte) (db.RowID,error) {
	values,err := service.QueryAllRowIdByIndex(key, value,1); if err != nil {
		return db.RowID(0),err
	}
	if len(values) > 0 {
		return values[0],nil
	}
	return db.RowID(0),nil
}

func (service *IndexService) QueryAllRowIdByIndex(key db.ColumnKey, value []byte, size int32) ([]db.RowID,error) {
	values,_,err := service.getIndexData(key, value, db.ASC, size,false); if err != nil {
		return nil,err
	}
	return service.primaryInsert.parse.RowIDList(values)
}