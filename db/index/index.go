package index

import (
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree/bptree"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type IndexService struct {
	storage *storage.IndexStorage
	iTree tree.TreeInterface
	treeHeadMap map[string]*tree.TreeHead
}

func NewIndexService(state state.ChainCodeState) *IndexService {
	var iTree tree.TreeInterface
	iTree = bptree.NewBPTreeImpl(storage.NewBPTreeStorage(state))
	treeHeadMap := map[string]*tree.TreeHead{}
	return &IndexService{storage.NewIndexStorage(state),iTree,treeHeadMap}
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

///////////////////// Common IndexData Function //////////////////////

func (service *IndexService) putIndexData(columnKey db.ColumnKey, key []byte, value []byte, insertType tree.InsertType) error {
	treeHead,err := service.getTreeHead(columnKey); if err != nil {
		return err
	}
	return service.iTree.Insert(treeHead, key, value, insertType)
}

func (service *IndexService) getIndexData(columnKey db.ColumnKey, key []byte) ([]byte,error) {
	treeHead,err := service.getTreeHead(columnKey); if err != nil {
		return nil,err
	}
	return service.iTree.Search(treeHead, key)
}

///////////////////// PrimaryKey Index Function //////////////////////

func (service *IndexService) PutPrimaryKeyIndex(database db.DataBaseID, table *db.TableData, rowID db.RowID, blockID db.BlockID) error {
	columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:table.PrimaryKey.ColumnID}
	return service.putIndexData(columnKey, util.RowIDToBytes(rowID), util.BlockIDToBytes(blockID), tree.InsertTypeAppend)
}

func (service *IndexService) GetPrimaryKeyIndex(database db.DataBaseID, table *db.TableData, rowID db.RowID) ([]db.BlockID,error) {
	columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:table.PrimaryKey.ColumnID}
	var blocks []db.BlockID
	bytes,err := service.getIndexData(columnKey, util.RowIDToBytes(rowID)); if err != nil {
		return nil,err
	}
	if len(bytes) > 0 {
		collection,err := tree.ParseCollectionByte(bytes); if err != nil {
			return nil,err
		}
		for _,v := range collection {
			blocks = append(blocks, util.BytesToBlockID(v))
		}
	}
	return blocks,nil
}

func (service *IndexService) GetPrimaryKeyIndexByLast(database db.DataBaseID, table *db.TableData, rowID db.RowID) (db.BlockID,error) {
	blocks,err := service.GetPrimaryKeyIndex(database, table, rowID); if err != nil {
		return 0,err
	}
	if len(blocks) > 0 {
		return blocks[len(blocks)-1],nil
	}
	return 0,nil
}

///////////////////// ForeignKey Index Function //////////////////////

func (service *IndexService) PutForeignKeysIndex(database db.DataBaseID, table *db.TableData, rowID db.RowID, row *db.RowData) error {
	for _,foreignKey := range table.ForeignKeys {
		value := row.Data[foreignKey.ColumnID-1]
		if len(value) > 0 {
			columnKey := db.ColumnKey{Database:database,Table:table.Id,Column:foreignKey.ColumnID}
			if err := service.putIndexData(columnKey, value, util.RowIDToBytes(rowID), tree.InsertTypeAppend); err != nil {
				return err
			}
		}
	}
	return nil
}

func (service *IndexService) GetForeignKeyIndex(database db.DataBaseID, tableID db.TableID, foreignKey db.ForeignKey, referenceRowID db.RowID) ([]db.RowID,error) {
	columnKey := db.ColumnKey{Database:database,Table:tableID,Column:foreignKey.ColumnID}
	var rows []db.RowID
	bytes,err := service.getIndexData(columnKey, util.RowIDToBytes(referenceRowID)); if err != nil {
		return nil,err
	}
	if len(bytes) > 0 {
		collection,err := tree.ParseCollectionByte(bytes); if err != nil {
			return nil,err
		}
		for _,v := range collection {
			rows = append(rows, util.BytesToRowID(v))
		}
	}
	return rows,nil
}

///////////////////// Other Index Function //////////////////////

func (service *IndexService) QueryRowIdByIndex(key db.ColumnKey, value []byte) (db.RowID,error) {
	values,err := service.QueryAllRowIdByIndex(key, value); if err != nil {
		return db.RowID(0),err
	}
	if values != nil && len(values) > 0 {
		return values[0],nil
	}
	return db.RowID(0),nil
}

func (service *IndexService) QueryAllRowIdByIndex(key db.ColumnKey, value []byte) ([]db.RowID,error) {
	var rowIds []db.RowID
	bytes,err := service.getIndexData(key, value); if err != nil {
		return nil,err
	}
	if len(bytes) > 0 {
		collection,err := tree.ParseCollectionByte(bytes); if err != nil {
			return nil,err
		}
		for _,v := range collection {
			rowIds = append(rowIds, util.BytesToRowID(v))
		}
	}
	return rowIds,nil
}