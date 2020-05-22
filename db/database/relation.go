package database

import (
	"fmt"
	"github.com/database-fabric/db"
)

func GetRelationKeysByReference(reference db.ReferenceKey, relation *db.Relation) ([]db.RelationKey,error) {
	keys := make([]db.RelationKey, 0, len(relation.Keys))
	for _,relationKey := range relation.Keys {
		if relationKey.IsDeleted {//过滤删除的关系
			continue
		}
		r := relationKey.ForeignKey.Reference
		if r.Equal(reference) {
			keys = append(keys, relationKey)
		}
	}
	return keys,nil
}

func AddRelationKey(key db.RelationKey, relation *db.Relation) error {
	//验证关系键是否存在
	for _,relationKey := range relation.Keys {
		if relationKey.Equal(key)  {
			if !relationKey.IsDeleted {//已存在一个非删除的关系，无法添加
				return fmt.Errorf("key already exists")
			}
		}
	}
	key.Id = db.RelationKeyID(len(relation.Keys)+1)
	relation.Keys = append(relation.Keys, key)
	return nil
}

func DeleteRelationKey(key db.RelationKey, relation *db.Relation) error {
	deleteID := db.RelationKeyID(0)
	if key.Id > 0 && key.Id <= db.RelationKeyID(len(relation.Keys)) {
		if relation.Keys[key.Id-1].Equal(key) {
			deleteID = key.Id
		}
	}
	if deleteID == 0 {
		for _,relationKey := range relation.Keys {
			if relationKey.Equal(key) {
				deleteID = relationKey.Id
			}
		}
	}
	if deleteID == 0 {
		return fmt.Errorf("key not found")
	}
	relation.Keys[deleteID-1].IsDeleted = true
	return nil
}