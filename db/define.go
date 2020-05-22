package db

import "github.com/database-fabric/protos/db/row"

const ChainPrefix = ""

type KeyType = uint8
const (
	ChainKeyType KeyType = iota
	DataBaseKeyType
	TableKeyType
	TallyKeyType
	RelationKeyType
	BlockKeyType
	IndexKeyType
)

type IndexType = uint8
const (
	BPTreeHeadIndexType IndexType = iota
	BPTreeNodeIndexType
	LinkedHeadIndexType
	LinkedNodeIndexType
)

type IndexValueType = uint8
const (
	ValueTypeNone IndexValueType = iota //空
	ValueTypeData //数据
	ValueTypePointer //指针
	ValueTypeCollection //集合
	ValueTypeLinkedList //链表
	ValueTypeTree //树
)

type DataType = uint8
const (
	UNDEFINED DataType = iota
	INT
	DECIMAL
	VARCHAR
	BOOL
)

type OpType = uint8
const (
	ADD OpType = iota
	UPDATE
	DELETE
)

type StateType = uint8
const (
	SetState StateType = iota
	DelState
)

type OrderType = uint8
const (
	ASC OrderType = iota
	DESC
)

//以下定义每个类型索引位置，使用ID别名，值从1开始，即对应索引为i-1
type DatabaseID = int8
type TableID = int16
type BlockID = int32
type RowID = int64
type ColumnID = int8
type RelationKeyID = int16

//表集合与表外键包装结构
type TableNames = map[TableID]string
type ForeignKeys = map[ColumnID]*ForeignKey

//行与记录块包装结构
type RowBlockID struct {
	RowID RowID
	BlockID BlockID
}
type RowDataHistory struct {
	TxID string `json:"txID"` //事务ID
	Time int64 `json:"time"` //事务时间戳
	Row *row.RowData
}

//索引kv结构
type KV struct {
	Key     []byte `json:"key"`
	Value   []byte `json:"value"`
	VType   IndexValueType `json:"vType"`
}

//时间戳类型
type Timestamp int64
//记录总数
type Total = int64

//接收外部任何kv结构数据
type JsonData = map[string]interface{}

//列键数据
type ColumnKey struct {
	Database DatabaseID `json:"database"`
	Table TableID `json:"table"`
	Column ColumnID `json:"column"`
}

//列键下行键数据
type ColumnRowKey struct {
	ColumnKey
	Row RowID `json:"row"`
}

type TableTally struct {
	TableID TableID `json:"tableID"`
	Increment RowID `json:"increment"`
	AddRow RowID `json:"addRow"`
	UpdateRow RowID `json:"updateRow"`
	DelRow RowID `json:"delRow"`
	Block BlockID `json:"block"`
}

type DataBase struct {
	Id DatabaseID `json:"id"`
	Relation *Relation `json:"relation"`
}

type Table struct {
	Data *TableData `json:"data"`
	Primary *Column `json:"primary"`
	ForeignKeys ForeignKeys `json:"foreignKeys"`
}

type Relation struct {
	Keys []RelationKey `json:"keys"`
}

type TableData struct {
	Id TableID `json:"id"`
	Name string `json:"name"`
	Columns []Column `json:"columns"`
	PrimaryKey PrimaryKey `json:"primaryKey"`
	ForeignKeys []ForeignKey `json:"foreignKeys"`
}

type Column struct {
	Id ColumnID `json:"id"`
	ColumnConfig
	IsDeleted bool `json:"isDeleted"`
	Order int8 `json:"order"`
}

type ColumnConfig struct {
	Name string `json:"name"`
	Type DataType `json:"type"`
	Default []byte `json:"default"`
	NotNull bool `json:"notNull"`
	Desc string `json:"desc"`
}

type PrimaryKey struct {
	ColumnID ColumnID `json:"columnID"`
	AutoIncrement bool `json:"autoIncrement"`
}

//外键关系表键
type ForeignKey struct {
	ColumnID ColumnID `json:"columnID"`
	Reference ReferenceKey `json:"reference"`
}

//外键主表键
type ReferenceKey struct {
	TableID TableID `json:""`
	ColumnID ColumnID `json:"columnID"`
}

//主外键关系
type RelationKey struct {
	Id RelationKeyID `json:"id"`
	IsDeleted bool `json:"isDeleted"`
	TableID TableID `json:"tableID"` //外键表
	ForeignKey ForeignKey `json:"foreignKey"` //外键关系表键
}

type Model struct {
	Name string `json:"name"`
	TableID TableID `json:"tableID"`
	IsArray bool `json:"isArray"`
	Models []Model `json:"models"`
}

type Schema struct {
	Name string `json:"name"`
	LayerNum int8 `json:"layerNum"`
	Model Model `json:"model"`
}

type Pagination struct {
	PageSize int32 `json:"pageSize"`
	Total Total `json:"total"`
	List []JsonData `json:"list"`
}

func (relationKey *RelationKey) Equal(key RelationKey) bool {
	return relationKey.TableID == key.TableID && relationKey.ForeignKey.Equal(key.ForeignKey)
}

func (foreignKey *ForeignKey) Equal(key ForeignKey) bool {
	return foreignKey.ColumnID == key.ColumnID && foreignKey.Reference.Equal(key.Reference)
}

func (referenceKey *ReferenceKey) Equal(key ReferenceKey) bool {
	return referenceKey.TableID == key.TableID && referenceKey.ColumnID == key.ColumnID
}
