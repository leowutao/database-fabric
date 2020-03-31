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
func (service *IndexService) getTreeHead(tableName string, columnName string) (*tree.TreeHead,error) {
	var err error
	name := tableName+"~"+columnName
	treeHead, ok := service.treeHeadMap[name]
	if ok {
		return treeHead,nil
	}
	treeHead,err = service.iTree.CreateHead(tableName, columnName, tree.TreeTypeDefault); if err != nil {
		return nil,err
	}
	service.treeHeadMap[name] = treeHead
	return treeHead,nil
}

func (service *IndexService) PutForeignKey(foreignKey db.ReferenceForeignKey) error {
	return service.storage.PutForeignKey(foreignKey)
}

func (service *IndexService) DelForeignKey(foreignKey db.ReferenceForeignKey) error {
	return service.storage.DelForeignKey(foreignKey)
}

func (service *IndexService) PutForeignKeyIndex(table db.Table, idValue string, row map[string]interface{}) error {
	for _,foreignKey := range table.ForeignKeys {
		column := foreignKey.Column
		value,err := util.ConvertString(row[column]); if err != nil {
			return err
		}
		if value != "" {
			treeHead,err := service.getTreeHead(table.Name, column); if err != nil {
				return err
			}
			if err := service.iTree.Insert(treeHead, []byte(value), []byte(idValue), tree.InsertTypeAppend); err != nil {
				return err
			}
		}
		//if err := service.storage.PutRowIdIndex(table.Name, column, value, idValue); err != nil {
		//	return err
		//}
	}

	return nil
}

func (service *IndexService) DelForeignKeyIndex(table db.Table, row map[string]interface{}) error {
	//for _,foreignKey := range table.ForeignKeys {
	//	column := foreignKey.Column
	//	value,err := util.ConvertString(row[column]); if err != nil {
	//		return err
	//	}
	//	if err := service.storage.DelRowIdIndex(table.Name, column, value); err != nil {
	//		return err
	//	}
	//}

	return nil
}

func (service *IndexService) QueryRowIdByIndex(tableName string, columnName string, columnData string) (string,error) {
	values,err := service.QueryAllRowIdByIndex(tableName, columnName, columnData); if err != nil {
		return "",err
	}
	if values != nil && len(values) > 0 {
		return values[0],nil
	}
	return "",nil
	//return service.storage.GetRowIdByIndex(tableName, columnName, columnData)
}

func (service *IndexService) QueryAllRowIdByIndex(tableName string, columnName string, columnData string) ([]string,error) {
	var rowIds []string
	treeHead,err := service.getTreeHead(tableName, columnName); if err != nil {
		return nil,err
	}
	value,err := service.iTree.Search(treeHead, []byte(columnData)); if err != nil {
		return nil,err
	}
	if value!= nil && len(value) > 0 {
		return tree.ParseCollectionString(value)
	}
	return rowIds,nil
	//return service.storage.GetAllRowIdByIndex(tableName, columnName, columnData)
}

func (service *IndexService) GetForeignKeyByReference(referenceTable string, referenceColumn string) (db.ReferenceForeignKey,error) {
	return service.storage.GetForeignKeyByReference(referenceTable, referenceColumn)
}