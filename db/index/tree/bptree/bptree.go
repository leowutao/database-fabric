package bptree

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

const (
	NODE_POINTER_SIZE = 4        //节点指针，pointer为int32类型
	MAX_NODE_SIZE     = 80 //1024 * 4 //节点最大容量4KB
	MAX_KEY_NUM       = 1000     //节点最大key数量，position为int16类型
	MAX_NODE_NUM      = 100000   //树最大节点数量
	MAX_TREE_HEIGHT   = 10        //树最大阶数

	NODE_SPLIT_RULE  = 2 //节点分裂规则：1为关键字个数验证
	MAX_NODE_KEY_NUM = 3 //MAX_TREE_HEIGHT
	MIN_NODE_KEY_NUM = MAX_NODE_KEY_NUM/2
	TREE_DEFAULT_TYPE = TreeTypeDefault

)

type Pointer = int32  //节点指针类型，占用4byte(节点关键字指向下级指针大小，如果关键字和下级指针类型一致，即占用8byte)
type Position = int16 //关键字位置，最大值32768(一个节点关键字数量不超过1000)

type TreeType int8

const (
	TreeTypeDefault TreeType = iota
	TreeTypeAsc
	TreeTypeDesc
)

type NodeType int8

const (
	NodeTypeRoot NodeType = iota
	NodeTypeChild
	NodeTypeLeaf
	NodeTypeData
)

/////////////////// Storage Struct Data ///////////////////
//表字段索引树头信息
type TreeHead struct {
	Table     string   `json:"table"`     //表名
	Column    string   `json:"column"`    //列名
	Type      TreeType `json:"type"`      //树类型，默认(无序，分裂按1/2规则)、升序、降序
	Root      Pointer  `json:"root"`      //根节点指针
	Height    int8     `json:"height"`    //高度
	NodeOrder int32    `json:"nodeOrder"` //当前节点累计自增序号
	NodeNum   int32    `json:"nodeNum"`   //节点数量
	KeyNum    int64    `json:"keyNum"`    //关键字数量
}

//节点数据，节点存储标识规则为：索引前缀(前缀+表名+字段名)+排序值(自增)
type TreeNode struct {
	Type   NodeType `json:"type"`   //节点类型，根、子、叶子、数据
	Keys   [][]byte `json:"keys"`   //关键字集合
	Values [][]byte `json:"values"` //关键字值集合(与关键字索引位置对应，格式为：指向下级指针或数据内容)
}

/////////////////// Logic Struct Data ///////////////////
type CompareType int8

const (
	CompareLt CompareType = iota
	CompareEq
	CompareGt
)

//节点所在位置
type TreeNodePosition struct {
	Pointer  Pointer           //节点指针
	Node     *TreeNode         //节点数据
	Parent   *TreeNodePosition //父级指针
	Position Position          //在父级的位置
}

//节点key所在位置
type TreeKeyPosition struct {
	NodePosition *TreeNodePosition //节点位置
	Position     Position          //key位置
	Compare      CompareType       //比较类型
}

//节点key所在位置和数据
type TreeKeyData struct {
	KeyPosition TreeKeyPosition //key位置
	Key         []byte          //匹配到的key
	Value       interface{}     //key对应的值
	ValueType   tree.ValueType  //值类型
}

//缓存树信息、已读到的节点集合和待写入的节点集合,减少io次数
type TreeNodeCache struct {
	Head    *TreeHead                     //树头
	IsWrite bool                          //写入需要缓存读写
	Read    map[Pointer]*TreeNodePosition //缓存读
	Write   map[Pointer]*TreeNodePosition //缓存写
}

type TreeNodeKV struct {
	Key   []byte
	Value []byte
}

func SplitMid(num Position) Position {
	n := num%2
	mid := num / 2
	if n > 0 {
		mid++
	}
	return mid
}

func BytesToPointer(value []byte) Pointer {
	return util.ByteToInt32(value)
}

func PointerToBytes(pointer Pointer) []byte {
	return util.Int32ToByte(pointer)
}

func ParseValue(value []byte) (interface{}, tree.ValueType, error) {
	var valueSlice []byte
	//目前定义一个字节来保存value的类型，用字节数组最后一位表示
	last := len(value) - 1
	valueType := value[last]
	valueSlice = value[:last]
	if valueType == tree.ValueTypeData { //数据
		return valueSlice, valueType, nil
	} else if valueType == tree.ValueTypePointer { //指针
		return BytesToPointer(valueSlice), valueType, nil
	} else if valueType == tree.ValueTypeCollection { //集合
		var collection [][]byte
		err := json.Unmarshal(valueSlice, &collection)
		if err != nil {
			return nil, valueType, fmt.Errorf("parse node value to collection error `%s`", err.Error())
		}
		return collection, valueType, nil
	}
	return nil, valueType, fmt.Errorf("parse node value tree.ValueType error")
}

func FormatValue(value interface{}, valueType tree.ValueType) ([]byte, error) {
	var convertValue []byte
	var err error
	if valueType == tree.ValueTypeData { //数据
		convertValue = value.([]byte)
	} else if valueType == tree.ValueTypePointer { //指针
		convertValue = PointerToBytes(value.(Pointer))
	} else if valueType == tree.ValueTypeCollection { //集合
		convertValue, err = util.ConvertJsonBytes(value.([][]byte))
		if err != nil {
			return nil, fmt.Errorf("format node value to collection error `%s`", err.Error())
		}
	} else {
		return nil, fmt.Errorf("format node value tree.ValueType error")
	}
	return append(convertValue, valueType), nil
}

func ToPointer(value interface{}) Pointer {
	return value.(Pointer)
}

func CreateTreeKeyData(nodePosition *TreeNodePosition, key []byte, value []byte, position Position, compare CompareType) (TreeKeyData, error) {
	convertValue, valueType, err := ParseValue(value)
	if err != nil {
		return TreeKeyData{}, err
	}
	if nodePosition.Node.Type == NodeTypeRoot || nodePosition.Node.Type == NodeTypeChild {
		if valueType != tree.ValueTypePointer {
			return TreeKeyData{}, fmt.Errorf("node value type must is tree.ValueTypePointer")
		}
	}
	return TreeKeyData{TreeKeyPosition{nodePosition, position, compare}, key, convertValue, valueType}, nil
}

func GetNodeSize(node *TreeNode) (int, error) {
	nodeBytes, err := util.ConvertJsonBytes(*node)
	if err != nil {
		return 0, err
	}
	// TODO 需要计算前缀大小
	nodeSize := len(nodeBytes) + NODE_POINTER_SIZE
	return nodeSize, nil
}

/**
	节点分裂规则(计算待插入key是否有空间)：1、key数量>=2 2、key数量>=最大数量 3、节点大小+插入key大小>最大容量
 */
func NodeIsSplit(node *TreeNode, kv *TreeNodeKV, height int8) (bool, error) {
	keyNum := len(node.Keys)
	if keyNum == 0 {//key数量必须2个以上才能分裂
		return false,nil
	}
	isSplit := false
	if NODE_SPLIT_RULE == 1 {
		if keyNum >= MAX_NODE_KEY_NUM {//当前数量已满，无法插入
			isSplit = true
		}
	} else {
		nodeSize, err := GetNodeSize(node)
		if err != nil {
			return false, err
		}
		// TODO  需要计算新加入一个key的大小，10代表额外的暂用(一般会暂用一个指针大小)
		addSize := len(kv.Key) + len(kv.Value) + 10
		if nodeSize+addSize > MAX_NODE_SIZE {
			isSplit = true
		}
	}
	if isSplit { //单个节点容量已满，做分裂
		if height == MAX_TREE_HEIGHT && node.Type == NodeTypeRoot { //根节点已满，无法新增
			return false, fmt.Errorf("root is full")
		}
	}
	return isSplit, nil
}

/*
	节点内部key二分查找
	返回最近节点key、最近key位置、比较结果，比较结果返回三种情况(等于：找到key位置，大于：右边，小于：左边)
*/
func BinarySearch(nodePosition *TreeNodePosition, key []byte) (TreeKeyData, error) {
	keyData := TreeKeyData{}
	keyData.KeyPosition = TreeKeyPosition{nodePosition, -1, CompareGt}
	keys := nodePosition.Node.Keys
	values := nodePosition.Node.Values
	keyNum := int16(len(keys))
	if keyNum == 0 {
		return keyData, nil
	}
	front := int16(0)
	end := keyNum - 1
	//对边界判定：=first、<first、=last、>last
	//如果只有一个值额外比较：>first
	first := keys[front]
	if bytes.Compare(key, first) == 0 {
		return CreateTreeKeyData(nodePosition, first, values[front], front, CompareEq)
	}
	if bytes.Compare(key, first) == -1 {
		return CreateTreeKeyData(nodePosition, first, values[front], front, CompareLt)
	}
	if front == end {
		if bytes.Compare(key, first) == 1 {
			return CreateTreeKeyData(nodePosition, first, values[front], front, CompareGt)
		}
	} else {
		last := keys[end]
		if bytes.Compare(key, last) == 0 {
			return CreateTreeKeyData(nodePosition, last, values[end], end, CompareEq)
		} else if bytes.Compare(key, last) == 1 {
			return CreateTreeKeyData(nodePosition, last, values[end], end, CompareGt)
		}
	}
	//节点范围(left<key<right)，选取以left节点位置(一个节点key对齐方式以左侧方式)
	//对齐方式针对中间key排列，实际最左和最右都为开区间
	for front < end {
		mid := (front + end) / 2
		current := keys[mid]
		if bytes.Compare(key, current) == 0 {
			return CreateTreeKeyData(nodePosition, current, values[mid], mid, CompareEq)
		} else if bytes.Compare(key, current) == 1 {
			next := keys[mid+1]
			if bytes.Compare(key, next) == -1 {
				return CreateTreeKeyData(nodePosition, current, values[mid], mid, CompareGt)
			}
			front = mid + 1
		} else {
			prevIndex := mid - 1
			prev := keys[prevIndex]
			if bytes.Compare(key, prev) == 1 {
				return CreateTreeKeyData(nodePosition, prev, values[prevIndex], prevIndex, CompareGt)
			}
			end = mid - 1
		}
	}
	return keyData, nil
}

func ConvertValue(value interface{}, insertType tree.InsertType, keyData *TreeKeyData) ([]byte, error) {
	// TODO value需要判定是否符合原值类型
	valueType := tree.ValueTypeData
	if keyData.KeyPosition.Compare == CompareEq { //存在情况需要判定插入类型
		if insertType == tree.InsertTypeDefault { //验证唯一性
			return nil, fmt.Errorf("key `%v` is already", keyData.Key)
		} else if insertType == tree.InsertTypeReplace { //替换原有值
			valueType = keyData.ValueType //类型必须符合原类型
		} else if insertType == tree.InsertTypeAppend { //在原值追加
			valueType = tree.ValueTypeCollection //类型变更为集合
			var collection [][]byte
			if keyData.ValueType != tree.ValueTypeCollection { //原值需要和当前值组合
				collection = append(collection, keyData.Value.([]byte))
				collection = append(collection, value.([]byte))
			} else { //原值为集合类型直接追加
				collection := keyData.Value.([][]byte)
				collection = append(collection, value.([]byte))
			}
			value = collection
		} else { //类型错误
			return nil, fmt.Errorf("insert type error")
		}
	}
	//不存在直接插入
	convertValue, err := FormatValue(value, valueType)
	if err != nil {
		return nil, err
	}
	return convertValue, nil
}

func TreeIsNull(head *TreeHead) bool {
	if head == nil || head.Root == 0 || head.Height == 0 {
		return true
	}
	return false
}

///////////////////// Private Function ////////////////////
func createNodePosition(nodeType NodeType, parent *TreeNodePosition, position Position, keys [][]byte, values [][]byte, cache *TreeNodeCache) (*TreeNodePosition, error) {
	if !cache.IsWrite {
		return nil, fmt.Errorf("createNodePosition error, cache must set isWrite is true")
	}
	pointer := cache.Head.NodeOrder + 1
	node := &TreeNode{nodeType, keys, values}
	nodePosition := &TreeNodePosition{pointer, node, parent, position}
	cache.Read[pointer] = nodePosition
	cache.Write[pointer] = nodePosition
	cache.Head.NodeOrder = pointer
	cache.Head.NodeNum++
	if nodeType == NodeTypeRoot {
		cache.Head.Root = pointer
		cache.Head.Height++
		cache.Head.KeyNum = cache.Head.KeyNum + int64(len(keys))
	}
	return nodePosition, nil
}

func createTreeNodeCache(head *TreeHead, isWrite bool) *TreeNodeCache {
	cache := &TreeNodeCache{}
	cache.Head = head
	cache.IsWrite = isWrite
	if isWrite {
		cache.Read = map[int32]*TreeNodePosition{}
		cache.Write = map[int32]*TreeNodePosition{}
	}
	return cache
}

/*
	调整树，节点容量超出需要递归分裂
*/
func balanceTree(splitData *TreeSplitData, cache *TreeNodeCache) error {
	isSplit, err := NodeIsSplit(splitData.Current.NodePosition.Node, splitData.InsertKV, cache.Head.Height)
	if err != nil {
		return err
	}
	if isSplit { //节点分裂，节点移动(具体看当前节点容量)
		err := splitNode(splitData, cache)
		if err != nil {
			return err
		}
		if splitData.Current != nil {
			err = balanceTree(splitData, cache)
			if err != nil {
				return err
			}
		}
	} else { //插入key
		//匹配到key或最近的key，写入key(可能产生移动)
		err = moveKey(splitData, cache)
		if err != nil {
			return err
		}
	}
	return nil
}

type TreeSplitData struct {
	Prev *TreeKeyPosition
	Current *TreeKeyPosition
	UpdateKV *TreeNodeKV
	InsertKV *TreeNodeKV
}

/*
	节点分裂，从叶子节点往上级分裂
*/
func splitNode(splitData *TreeSplitData, cache *TreeNodeCache) error {
	prevKeyPosition := splitData.Prev
	keyPosition := splitData.Current
	kv := splitData.InsertKV
	nodePosition := keyPosition.NodePosition
	node := nodePosition.Node
	position := nodePosition.Position
	compare := keyPosition.Compare
	isRoot := cache.Head.Height == 1 || node.Type == NodeTypeRoot
	if !isRoot && compare == CompareLt && keyPosition.Position == 0 { //最左边,如果是根节点，必须按1/2分裂
		if prevKeyPosition != nil { //对应的子节点位置修改为0
			prevKeyPosition.Position = 0
		}
		leftNodePosition, err := createNodePosition(node.Type, nodePosition.Parent, position-1, [][]byte{kv.Key}, [][]byte{kv.Value}, cache)
		if err != nil {
			return  err
		}
		parentV, err := FormatValue(leftNodePosition.Pointer, tree.ValueTypePointer)
		if err != nil {
			return fmt.Errorf("split node error `%s`", err)
		}
		splitData.Prev = keyPosition
		splitData.Current = &TreeKeyPosition{nodePosition.Parent, nodePosition.Position, compare}
		splitData.UpdateKV = nil
		splitData.InsertKV = &TreeNodeKV{kv.Key, parentV}
	} else { //排序树和默认树都往右边分裂出新节点
		isOrder := cache.Head.Type == TreeTypeAsc || cache.Head.Type == TreeTypeDesc
		isBinary := false
		var rightKeys [][]byte
		var rightValues [][]byte
		if isOrder && compare == CompareGt && keyPosition.Position+1 == int16(len(node.Keys)) { //只有排序树到最右边开始分裂
			if prevKeyPosition != nil { //对应的子节点位置修改为0
				prevKeyPosition.Position = 0
			}
			rightKeys = [][]byte{kv.Key}
			rightValues = [][]byte{kv.Value}
		} else { //其他默认按1/2分裂
			err := moveKey(splitData, cache); if err != nil {
				return  err
			}
			isBinary = true
			keyNum := int16(len(node.Keys))
			mid := SplitMid(keyNum)
			leftKeys := node.Keys[:mid]
			leftValues := node.Values[:mid]
			rightKeys = node.Keys[mid:]
			rightValues = node.Values[mid:]
			node.Keys = leftKeys
			node.Values = leftValues
		}
		rightNodePosition, err := createNodePosition(node.Type, nodePosition.Parent, position+1, rightKeys, rightValues, cache)
		if err != nil {
			return err
		}
		var parentLeftKey = node.Keys[0]
		parentLeftValue, err := FormatValue(nodePosition.Pointer, tree.ValueTypePointer)
		if err != nil {
			return fmt.Errorf("split node error `%s`", err)
		}
		parentRightKey := rightKeys[0]
		parentRightValue, err := FormatValue(rightNodePosition.Pointer, tree.ValueTypePointer)
		if err != nil {
			return fmt.Errorf("split node error `%s`", err)
		}
		if isRoot { //当前节点为根节点，需要创建新根节点
			rootKeys := [][]byte{parentLeftKey, parentRightKey}
			rootValues := [][]byte{parentLeftValue, parentRightValue}
			rootNodePosition, err := createNodePosition(NodeTypeRoot, nil, 0, rootKeys, rootValues, cache)
			if err != nil {
				return err
			}
			nodePosition.Position = Position(0)
			nodePosition.Parent = rootNodePosition
			rightNodePosition.Position = Position(1)
			rightNodePosition.Parent = rootNodePosition
			if node.Type == NodeTypeRoot {
				nodePosition.Node.Type = NodeTypeChild
				rightNodePosition.Node.Type = NodeTypeChild
			}
			splitData.Prev = nil
			splitData.Current = nil
			splitData.UpdateKV = nil
			splitData.InsertKV = nil
		} else { //右边节点的第一个key需要插入到父级中(需要做移动)
			if nodePosition.Parent == nil {
				return fmt.Errorf("node `%d` parent cache read miss", nodePosition.Pointer)
			}
			splitData.Prev = keyPosition
			splitData.Current = &TreeKeyPosition{nodePosition.Parent, position, CompareGt}
			if isBinary {//二分法分裂原key需要更新为左边节点的新key
				splitData.UpdateKV = &TreeNodeKV{parentLeftKey, parentLeftValue}
			}else{
				splitData.UpdateKV = nil
			}
			splitData.InsertKV = &TreeNodeKV{parentRightKey, parentRightValue}
		}
	}
	return nil
}

/*
	节点内部key变动
*/
func moveKey(splitData *TreeSplitData, cache *TreeNodeCache) error {
	keyPosition := splitData.Current
	updateKV := splitData.UpdateKV
	insertKV := splitData.InsertKV
	node := keyPosition.NodePosition.Node
	position := keyPosition.Position
	compare := keyPosition.Compare
	isWrite := false
	if updateKV != nil { //变更
		node.Keys[position] = updateKV.Key
		node.Values[position] = updateKV.Value
		isWrite = true
	}
	if insertKV != nil && compare != CompareEq { //插入(除插入到最右边不需要移动，其他情况都需要移动)
		keyNum := int16(len(node.Keys))
		if position+1 == keyNum { //插入到最右边
			node.Keys = append(node.Keys, insertKV.Key)
			node.Values = append(node.Values, insertKV.Value)
		} else { //插入中间，需要移动右边元素
			move := position          //默认移动右边元素，即插入在右边，当前元素无需移动
			if compare == CompareLt { //插入到左边，当前元素需要右移动，填充到当前位置
				if move > 0 {//防止下标为负数,最左边为0
					move--
				}
			} else if compare == CompareGt { //插入到右边，填充到右边位置
				position++
			}
			//动态增加数组长度
			node.Keys = append(node.Keys, nil)
			node.Values = append(node.Values, nil)
			//从move位置之后所有元素往右移动
			for i := keyNum; i > move; i-- {
				node.Keys[i] = node.Keys[i-1]
				node.Values[i] = node.Values[i-1]
			}
			node.Keys[position] = insertKV.Key
			node.Values[position] = insertKV.Value
		}
		cache.Head.KeyNum++
		isWrite = true
	}
	if isWrite {
		//缓存待写入节点
		cache.Write[keyPosition.NodePosition.Pointer] = keyPosition.NodePosition
	}else{
		return fmt.Errorf("moveKey write rule not found")
	}
	return nil
}