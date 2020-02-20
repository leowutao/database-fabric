package db

import "github.com/hyperledger/fabric/core/chaincode/shim"

type DbManager struct {
	ChainCodeStub shim.ChaincodeStubInterface
	CacheData map[string][]byte
}

type DataType int8
const (
	UNDEFINED DataType = iota
	INT
	DECIMAL
	VARCHAR
	BOOL
)

type OpType int8
const (
	ADD OpType = iota
	UPDATE
	DELETE
)

type Table struct {
	Name string `json:"name"`
	Columns []Column `json:"columns"`
	PrimaryKey PrimaryKey `json:"primaryKey"`
	ForeignKeys []ForeignKey `json:"foreignKeys"`
}

type PrimaryKey struct {
	Column string `json:"column"`
	AutoIncrement bool `json:"autoIncrement"`
}

type ForeignKey struct {
	Column string `json:"column"`
	Reference ReferenceKey `json:"reference"`
}

type ReferenceKey struct {
	Table string `json:"table"`
	Column string `json:"column"`
}

type ReferenceForeignKey struct {
	Reference ReferenceKey `json:"reference"`
	ForeignKey ReferenceKey `json:"foreignKey"`
}

type Column struct {
	Name string `json:"name"`
	Type DataType `json:"type"`
	Default interface{} `json:"default"`
	NotNull bool `json:"notNull"`
	Desc string `json:"desc"`
}

type Model struct {
	Name string `json:"name"`
	Table string `json:"table"`
	IsArray bool `json:"isArray"`
	Models []Model `json:"models"`
}

type Schema struct {
	Name string `json:"name"`
	LayerNum int8 `json:"layerNum"`
	Model Model `json:"model"`
}

type TableTally struct {
	Increment int64 `json:"increment"`
	Count int64 `json:"count"`
}

type Pagination struct {
	PageSize int32 `json:"pageSize"`
	Total int64 `json:"total"`
	List []interface{} `json:"list"`
}