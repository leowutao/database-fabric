package db

//根据datatype分类，定义层级关系，key规则为上级索引+自己的索引
//行数据结构定义：表管理所有行索引数据，多个行索引组成一个block或一个行索引分割多个block)，表对每个block定义大小，设置block头(行索引数量、行计数区间)
//表管理所有block，每个block对应的数据大小为4kb，一个表管理的block数量需要定义
//block格式定义(防止单行数据过大，建立双向链表来查找出所有的块)：上一个相邻块、下一个相邻块、数据字节二维素组、可用容量
//行结构定义：表将所有列抽象成数组，行数据压缩成二维字节数组，行数据数组下标与列中的下标对应
//如果列修改或删除如何定位：理论上列是不做删除，只追加，通过修改表中列的排列来减少数据移动位置，读取到数据需要按列位置来排列，还需要过滤删除的列数据
//修改列类型无法影响行数据，也不检查行数据类型，查询会对列类型数据做转换
//所有写入尽量轻，查询可以稍微重些，由于查询到的数据可能需要二次过滤，实际上需要通过客户端来调用合约查询，而且查询的数据量不会太大，如果需要做分析，则是将数据dump出去
//查询慢是针对外部，内部则是尽量不做变更保存原始数据查询
//行结构：行id、行数据列表、切割位置(如果行数据大于block容量，记录切割索引)
//行数据中的一列数据如何切割：
//历史版本：行数据使用block来记录，通过主键索引叶子节点数据关联多个版本block记录，表记录使用现有的history对象记录

const ChainPrefix = ""

type KeyType int8
const (
	ChainKeyType KeyType = iota
	DataBaseKeyType
	TableKeyType
	RelationKeyType
	BlockKeyType
	IndexKeyType
	RowKeyType
	SchemaKeyType
	TallyKeyType
	ForeignKeyKeyType
	HistoryKeyType
)

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

type StateType = int8
const (
	SetState StateType = iota
	DelState
)

//以下定义每个类型索引位置，使用ID别名，值从1开始，即对应索引为i-1
type DataBaseID = int8
type TableID = int16
type BlockID = int32
type RowID = int64
type ColumnID = int8
type RelationKeyID = int16

type TableNames = map[TableID]string
type JsonData = map[string]interface{}
type ForeignKeys = map[ColumnID]*ForeignKey


type ColumnKey struct {
	Database DataBaseID `json:"database"`
	Table TableID `json:"table"`
	Column ColumnID `json:"column"`
}

//行块数据(行集合数据)
type BlockData struct {
	Id BlockID `json:"id"`
	TxID string `json:"txID"` //事务ID
	Timestamp int64 `json:"timestamp"` //事务时间戳
	Rows []RowData `json:"rows"` //行数据列表
	SplitPosition int16 `json:"splitPosition"` //记录行列表最后一条拆分行数据位置(从1开始)，方便多个块之间行数据连接
}

//行数据
type RowData struct {
	Id RowID `json:"id"` //行ID
	Op OpType `json:"op"` //操作类型
	Data [][]byte `json:"data"` //行数据，代表多列数据列表，列表中索引与表设置列的位置对应
}

type TableTally struct {
	Increment RowID `json:"increment"`
	Row RowID `json:"row"`
	AddRow RowID `json:"addRow"`
	UpdateRow RowID `json:"updateRow"`
	DelRow RowID `json:"delRow"`
	Block BlockID `json:"block"`
}

type DataBase struct {
	Id DataBaseID `json:"id"`
	Relation *Relation `json:"relation"`
}

type Table struct {
	Data *TableData `json:"data"`
	PrimaryName string `json:"primaryName"`
	ForeignKeys ForeignKeys `json:"foreignKeys"`
}

type Relation struct {
	Keys []RelationKey `json:"keys"`
}

type TableData struct {
	Id TableID `json:"id"`
	Name string `json:"name"`
	Columns []Column `json:"columns"`
	DelColumns []ColumnID `json:"delColumns"`
	PrimaryKey PrimaryKey `json:"primaryKey"`
	ForeignKeys []ForeignKey `json:"foreignKeys"`
	Tally TableTally `json:"tally"`
}

type Column struct {
	Id ColumnID `json:"id"`
	IsDeleted bool `json:"isDeleted"`
	Name string `json:"name"`
	Type DataType `json:"type"`
	Default []byte `json:"default"`
	NotNull bool `json:"notNull"`
	Order int8 `json:"order"`
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
	Total RowID `json:"total"`
	List []interface{} `json:"list"`
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
