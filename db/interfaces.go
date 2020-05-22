package db

import (
	"github.com/database-fabric/protos/db/row"
)

type DatabaseInterface interface {
	GetRelation() (*Relation,error)
	GetRelationKeysByReference(reference ReferenceKey) ([]RelationKey,error)

	GetTableTally(tableID TableID) (*TableTally,error)

	GetTableName(tableID TableID) (string,error)
	GetTableID(tableName string) (TableID,error)

	CreateTableData(table *TableData) (TableID,error)
	UpdateTableData(table *TableData) error
	DeleteTableData(tableID TableID) error

	QueryTableDataByName(tableName string) (*TableData,error)
	QueryTableDataByID(tableID TableID) (*TableData,error)

	AddRowData(table *TableData, rows []*row.RowData) error

	QueryRowBlockID(table *TableData, rowID RowID) (BlockID,error)
	QueryRowData(table *TableData, rowID RowID) (*row.RowData,error)
	QueryRowIDByForeignKey(tableID TableID, foreignKey ForeignKey, referenceRowID RowID, size int32) ([]RowID,error)

	QueryRowDataByRange(table *TableData, start RowID, end RowID, order OrderType, size int32) ([]*row.RowData,error)

	QueryRowDataHistoryByRange(table *TableData, rowID RowID, order OrderType, size int32) ([]*RowDataHistory,Total,error)
}

