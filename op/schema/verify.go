package schema

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
)

func (service *SchemaService) ValidateSchemaExists(schemaName string) error {
	bytes,err := service.storage.GetSchemaDataByFilter(schemaName,true)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return fmt.Errorf("schema `%s` not exists", schemaName)
	}
	return nil
}

func (service *SchemaService) ValidateSchemaNotExists(schemaName string) error {
	bytes,err := service.storage.GetSchemaDataByFilter(schemaName,true)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		return fmt.Errorf("schema `%s` already exists", schemaName)
	}
	return nil
}

func (service *SchemaService) ValidateQuerySchemaIsNotNull(schemaName string) (db.Schema,error) {
	data,err := service.querySchema(schemaName)
	if err != nil {
		return data,err
	}
	if data.LayerNum == 0 {
		return data,fmt.Errorf("schema `%s` is null", schemaName)
	}

	return data,nil
}