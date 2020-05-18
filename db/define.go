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
/*

/**

TODO 叶子节点索引数据支持双向链表节点数据
1、对叶子节点关联指针数据过多，建立下级索引节点数据，叶子节点数据类型由集合转变为链表类型
2、原叶子节点数据全部移入到新链表节点中，原叶子节点数据存储链表头指针、尾指针、链表关键字个数
3、链表节点结构：上级指针、下级指针、值列表

TODO 叶子节点索引结构支持历史版本
1、需要指定记录排序，叶子节点数据如果指向链表，需要记录最新插入的数据，数据格式可以由外部定义，需要支持外部提供的两个自定义方法，数据格式定义方法、数据验证方式方法

TODO 数据压缩
1、叶子节点压缩：数据可能非常大，如果建立二层链表结构底层链表数据需要压缩
2、关键字压缩：其他类型节点按4kb实际计算只能存100多个关键字，如何存更多关键字需要压缩

每次行事务受影响的key类型为：
写：TallyKeyType、BlockKeyType、IndexKeyType(BPTreeHeadIndexType、BPTreeNodeIndexType)
读：ChainKeyType、DataBaseKeyType、TableKeyType、RelationKeyType、TallyKeyType、BlockKeyType、IndexKeyType
以上可以计算出每次行事务读大约在8-16个key，写大约在4-12个key，key共交互12-28次
按每个key存4kb计算：读=32kb～64kb，写=8kb～48kb，总共=40kb～112kb
写目前放大还算小，由于写入数据会组合或拆分多个block，所以批量添加小数据行或者大数据行比较适合，能够利用索引和block

优化：能否写控制在4次以下 ，读控制在8次以下，总交互大小控制在40kb以下，相比较单key交互10kb左右只放大4倍
 */

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

type JoinType = uint8
const (
	JoinTypeNone JoinType = iota //不连接
	//(存在分裂，记录行列表最后一条行部分数据包含在下一个块中)
	JoinTypeRow //行值连接
	JoinTypeColumn //列值连接
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
	Tx *TxData
	Row *RowData
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

//事务数据
type TxData struct {
	TxID string `json:"txID"` //事务ID
	Time Timestamp `json:"time"` //事务时间戳
}

//行块数据(行集合数据)
type BlockData struct {
	Id BlockID `json:"id"`
	TxData
	Rows []RowData `json:"rows"` //行数据列表
	Join JoinType `json:"join"` //块连接方式
}

//行数据
type RowData struct {
	Id RowID `json:"id"` //行ID
	Op OpType `json:"op"` //操作类型
	Data [][]byte `json:"data"` //行数据，代表多列数据列表，与表列对齐
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
	ColumnData
	IsDeleted bool `json:"isDeleted"`
	Order int8 `json:"order"`
}

type ColumnData struct {
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
