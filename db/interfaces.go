package db

type DatabaseInterface interface {
	CreateTableData(table *TableData) (TableID,error)
	UpdateTableData(table *TableData) error
	DeleteTableData(tableID TableID) error

	QueryTableDataByName(tableName string) (*TableData,error)
	QueryTableDataByID(tableID TableID) (*TableData,error)

	GetTableName(tableID TableID) (string,error)
	GetTableID(tableName string) (TableID,error)

	AddRowData(table *TableData, rows []*RowData) error

	QueryRowBlockID(table *TableData, rowID RowID) (BlockID,error)
	QueryRowData(table *TableData, rowID RowID) (*RowData,error)
	QueryRowIDByForeignKey(tableID TableID, foreignKey ForeignKey, referenceRowID RowID) ([]RowID,error)

	GetRelation() (*Relation,error)
	GetRelationKeysByReference(reference ReferenceKey) ([]RelationKey,error)
}

