package db

import (
	"bytes"
	"encoding/json"
	"fmt"
)

//申请存储空间
//创建根节点
//创建子节点
//创建叶子节点

//如何合并写，leveldb每次写入是需要新增log，之后在同步更新到磁盘，log文件可能会非常大，比如一个设计一个存储4K容量的索引节点，节点的关键字是8哥字节加上指针8字节，
//总共16字节，插入次数为c=(4*1024/16)=256，总共需要占用日志空间为:16*(c^2+c)=1052672(byte)=1028(kb)=1m
//一个节点最大256个关键字，做一个三阶数，256^3=16777216

//插入，从根节点查找插入的节点，排序方式按从上至下，从左往右
//分裂，每次计算

//分裂：如果有一个节点有 2d 个 key，增加一个后为 2d+1 个 key，不符合上述规则 B-树的每个节点有 d~2d 个 key，大于 2d，则将该节点进行分裂，分裂为两个 d 个 key 的节点并将中值 key 归还给父节点。
//合并：如果有一个节点有 d 个 key，删除一个后为 d-1 个 key，不符合上述规则 B-树的每个节点有 d~2d 个 key，小于 d，则将该节点进行合并，合并后若满足条件则合并完成，不满足则均分为两个节点。


//结构定义:每个节点保存key和value字节数组列表，节点类型需要区分类型(根、子、叶子)，默认情况下唯一的节点定为根节点
//节点存储key规则为索引前缀(前缀+表名+字段名)+排序值(每次增加节点设置的自增),value存储规则为索引key和索引value对应关系
//节点的自增排序值默认实现双向链表，在一个有序自增的列表中，所有连接方式都默认实现，但如果中间新增或删除节点会影响链表，。。。
//索引key存储数据表中的字段值，索引value规则:根、子节点存储下级节点排序值，叶子节点存储主键值列表
//存储每个索引头信息:索引前缀(前缀+表名+字段名)+HEAD，存储整个索引树的阶数、节点总数、节点key总数、每阶节点总数、key总数列表

//查找：从根开始递归到叶子，每个节点需要用二分查找出该节点key引用到下级节点，直至找到叶子的位置
//插入：根据查找的位置进行插入，此处插入位置以节点最后一个key为来区分为左右，由于树排序由左至右，故插入到右为追加，插入到左需要向右移动
//右移动：按插入位置之后的元素全部往右移动
//分裂：当插入到节点该节点容量超出之后将进行二分法分裂，分裂之后此阶层排序值变更
//分裂叶子：定位上级节点key位置(可根据key引用值匹配到位置)，将左节点最后一个key插入到上级节点的左边，将右节点第一个key插入到上级节点的右边
//分裂子：通过key引用值匹配来递归上级

const (
	MAX_NODE_SIZE = 1024*4 //节点最大容量4KB
	MAX_KEY_NUM = 1000 //节点最大key数量，position为int16类型
	MAX_NODE_NUM = 100000 //树最大节点数量
	MAX_TREE_HEIGHT = 4 //树最大阶数
	NODE_POINTER_SIZE = 4 //节点指针，pointer为int32类型

	NODE_SPLIT_RULE = 1 //节点分裂规则：1为关键字个数验证
	MAX_NODE_KEY_NUM = 51
)

type Pointer = int32//节点指针类型，占用4byte(节点关键字指向下级指针大小，如果关键字和下级指针类型一致，即占用8byte)
type Position = int16//关键字位置，最大值32768(一个节点关键字数量不超过1000)

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
	Table string `json:"table"`//表名
	Column string `json:"column"`//列名
	Root Pointer `json:"root"`//根节点指针
	Height int8 `json:"height"`//高度
	NodeOrder int32 `json:"nodeOrder"`//当前节点累计自增序号
	NodeNum int32 `json:"nodeNum"`//节点数量
	KeyNum int64 `json:"keyNum"`//关键字数量
}

//节点数据，节点存储标识规则为：索引前缀(前缀+表名+字段名)+排序值(自增)
type TreeNode struct {
	Pointer Pointer `json:"pointer"`//节点指针
	Type NodeType `json:"type"`//节点类型，根、子、叶子、数据
	Parent Pointer `json:"parent"`//父级指针
	Position Position `json:"position"`//在父级的位置
	Keys [][]byte `json:"keys"`//关键字集合
	Values [][]byte `json:"values"`//关键字值集合(与关键字索引位置对应，格式为：指向下级指针或数据内容)

}

/////////////////// Logic Struct Data ///////////////////
type CompareType int8
const (
	CompareLt CompareType = iota
	CompareEq
	CompareGt
)

type NodeValueType = uint8
const (
	NodeValueTypeData NodeValueType = iota
	NodeValueTypeCollection
	NodeValueTypePointer
)

type InsertType int8
const (
	InsertTypeDefault InsertType = iota
	InsertTypeReplace
	InsertTypeAppend
)

//节点key所在位置
type TreeKeyPosition struct {
	node *TreeNode
	key []byte
	value interface{}
	valueType NodeValueType
	position Position
	compare CompareType
}

//缓存树信息、已读到的节点集合和待写入的节点集合,减少io次数
type TreeNodeCache struct {
	head *TreeHead
	isWrite bool
	read map[Pointer]*TreeNode
	write map[Pointer]*TreeNode
}

func (t *DbManager) bytesToPointer(value []byte) Pointer {
	return t.ByteToInt32(value)
}

func (t *DbManager) pointerToBytes(pointer Pointer) []byte {
	return t.Int32ToByte(pointer)
}

func (t *DbManager) parseValue(value []byte) (interface{},NodeValueType,error) {
	var valueSlice []byte
	//目前定义一个字节来保存value的类型，用字节数组最后一位表示
	last := len(value)-1
	valueType := value[last]
	valueSlice = value[:last]
	if valueType == NodeValueTypeData {//数据
		return valueSlice,valueType,nil
	}else if valueType == NodeValueTypePointer {//指针
		return t.bytesToPointer(valueSlice),valueType,nil
	}else if valueType == NodeValueTypeCollection {//集合
		var collection [][]byte
		err := json.Unmarshal(valueSlice, &collection); if err !=nil {
			return nil,valueType,fmt.Errorf("parse node value to collection error `%s`", err.Error())
		}
		return collection,valueType,nil
	}
	return nil,valueType,fmt.Errorf("parse node value nodeValueType error")
}

func (t *DbManager) formatValue(value interface{}, valueType NodeValueType) ([]byte,error) {
	var convertValue []byte
	var err error
	if valueType == NodeValueTypeData {//数据
		convertValue = value.([]byte)
	}else if valueType == NodeValueTypePointer {//指针
		convertValue = t.pointerToBytes(value.(Pointer))
	}else if valueType == NodeValueTypeCollection {//集合
		convertValue,err = t.ConvertJsonBytes(value.([][]byte))
		if err != nil {
			return nil,fmt.Errorf("format node value to collection error `%s`", err.Error())
		}
	}else{
		return nil,fmt.Errorf("format node value nodeValueType error")
	}
	return append(convertValue, valueType),nil
}

func (t *DbManager) toPointer(value interface{}) Pointer {
	return value.(Pointer)
}

func (t *DbManager) createTreeKeyPosition(node *TreeNode, key []byte, value []byte, position Position, compare CompareType) (TreeKeyPosition,error) {
	convertValue,valueType,err := t.parseValue(value); if err != nil {
		return TreeKeyPosition{},err
	}
	return TreeKeyPosition{node,key,convertValue,valueType,position,compare},nil
}

func (t *DbManager) getHead(table string, column string, cache *TreeNodeCache) (*TreeHead,error) {
	if cache.head != nil {
		return cache.head,nil
	}
	headBytes,err := t.getBPTreeHead(table, column); if err != nil {
		return nil,err
	}
	var head *TreeHead
	if headBytes == nil || len(headBytes) == 0 {
		head = &TreeHead{table,column,0,0,0,0,0}
	}else{
		head = &TreeHead{}
		if err := json.Unmarshal(headBytes, head); err != nil {
			return nil,err
		}
	}
	cache.head = head
	return head,nil
}

func (t *DbManager) getNode(pointer Pointer, cache *TreeNodeCache) (*TreeNode,error) {
	node,ok := cache.read[pointer]
	if ok {
		return node,nil
	}
	nodeBytes,err := t.getBPTreeNode(cache.head.Table, cache.head.Column, t.Int64ToString(int64(pointer))); if err != nil {
		return nil,err
	}
	if nodeBytes == nil || len(nodeBytes) == 0 {
		return nil,fmt.Errorf("node `%d` not found", pointer)
	}else{
		node = &TreeNode{}
		if err := json.Unmarshal(nodeBytes, node); err != nil {
			return nil,err
		}
	}
	if cache.isWrite {
		cache.read[pointer] = node
	}
	return node,nil
}

func (t *DbManager) putNode(cache *TreeNodeCache) error {
	table := cache.head.Table
	column := cache.head.Column
	for pointer,node := range cache.write {
		nodeBytes, err := t.ConvertJsonBytes(*node); if err != nil {
			return err
		}
		if err := t.putBPTreeNode(table, column, t.Int64ToString(int64(pointer)), nodeBytes); err != nil {
			return err
		}
	}
	headBytes, err := t.ConvertJsonBytes(*cache.head); if err != nil {
		return err
	}
	if err := t.putBPTreeHead(table, column, headBytes); err != nil {
		return err
	}
	return nil
}

func (t *DbManager) createNode(nodeType NodeType, parent Pointer, position Position, keys [][]byte, values [][]byte, cache *TreeNodeCache) *TreeNode {
	pointer := cache.head.NodeOrder + 1
	node := &TreeNode{pointer,nodeType,parent,position,keys,values}
	cache.read[pointer] = node
	cache.write[pointer] = node
	cache.head.NodeOrder = pointer
	cache.head.NodeNum++
	return node
}

func (t *DbManager) getNodeSize(node *TreeNode) (int,error) {
	nodeBytes,err := t.ConvertJsonBytes(*node); if err != nil {
		return 0,err
	}
	nodeSize := len(nodeBytes) + NODE_POINTER_SIZE
	return nodeSize,nil
}

func (t *DbManager) nodeSizeIsMax(node *TreeNode, cache *TreeNodeCache) (bool,error) {
	isMax := false
	if NODE_SPLIT_RULE == 1 {
		if len(node.Keys) >= MAX_NODE_KEY_NUM {
			isMax = true
		}
	}else{
		nodeSize,err := t.getNodeSize(node); if err != nil {
			return false,err
		}
		if nodeSize >= MAX_NODE_SIZE {
			isMax = true
		}
	}
	if isMax { //单个节点容量已满，做分裂
		if cache.head.Height == MAX_TREE_HEIGHT && node.Type == NodeTypeRoot {//根节点已满，无法新增
			return false,fmt.Errorf("root is full")
		}
		return true,nil
	}
	return false,nil
}

func (t *DbManager) createTreeNodeCache(head *TreeHead, isWrite bool) *TreeNodeCache {
	cache := &TreeNodeCache{}
	cache.head = head
	cache.isWrite = isWrite
	if isWrite {
		cache.read = map[int32]*TreeNode{}
		cache.write = map[int32]*TreeNode{}
	}
	return cache
}

/*
	查询key所在叶子节点位置
	从根开始递归查找(用二分法比较key大小)，直到找到叶子节点
	返回最近节点、最近节点key、最近key位置、比较结果，比较结果返回三种情况(等于：找到key位置，大于：右边，小于：左边)
 */
func (t *DbManager) findPosition(key []byte, cache *TreeNodeCache) (*TreeKeyPosition,error) {
	var err error
	var keyPosition *TreeKeyPosition
	head := cache.head

	rootNode,err := t.getNode(head.Root, cache); if err != nil {
		return nil,err
	}

	if head.Height == 1 {//树阶数为1，叶子节点为根节点
		var position TreeKeyPosition
		position,err = t.binarySearch(rootNode, key)
		keyPosition = &position
	}else{//从根遍历查找到最底层叶子节点位置
		keyPosition,err = t.recursionNode(rootNode, head.Height, key, cache)
	}
	if err != nil {
		return nil,err
	}
	//写入查找时需要缓存查找的节点和待写入的叶子节点
	if cache.isWrite {
		cache.write[keyPosition.node.Pointer] = keyPosition.node
	}
	return keyPosition,nil
}

func (t *DbManager) recursionNode(node *TreeNode, height int8, key []byte, cache *TreeNodeCache) (*TreeKeyPosition,error) {
	if len(node.Keys) == 0 {//节点为空，无法递归
		return nil,fmt.Errorf("recursionNode node `%d` is null", node.Pointer)
	}
	keyPosition,err := t.binarySearch(node, key); if err != nil {
		return nil,err
	}
	if node.Type == NodeTypeLeaf {//递归到最底层叶子节点
		return &keyPosition,nil
	}else{//递归查找下级节点
		lowerNode,err := t.getNode(t.toPointer(keyPosition.value), cache); if err != nil {
			return nil,err
		}
		return t.recursionNode(lowerNode, height, key, cache)
	}
}

/*
	节点内部key二分查找
	返回最近节点key、最近key位置、比较结果，比较结果返回三种情况(等于：找到key位置，大于：右边，小于：左边)
 */
func (t *DbManager) binarySearch(node *TreeNode, key []byte) (TreeKeyPosition,error) {
	keyPosition := TreeKeyPosition{node,nil,nil,0,-1,CompareGt}
	keys := node.Keys
	values := node.Values
	keyNum := int16(len(keys))
	if keyNum == 0 {
		return keyPosition,nil
	}
	front := int16(0)
	end := keyNum-1
	//对边界判定：=first、<first、=last、>last
	//如果只有一个值额外比较：>first
	first := keys[front]
	if bytes.Compare(key, first) == 0 {
		return t.createTreeKeyPosition(node, first, values[front], front, CompareEq)
	}
	if bytes.Compare(key, first) == -1 {
		return t.createTreeKeyPosition(node, first, values[front], front, CompareLt)
	}
	if front == end {
		if bytes.Compare(key, first) == 1 {
			return t.createTreeKeyPosition(node, first, values[front], front, CompareGt)
		}
	}else {
		last := keys[end]
		if bytes.Compare(key, last) == 0 {
			return t.createTreeKeyPosition(node, last, values[end], end, CompareEq)
		}else if bytes.Compare(key, last) == 1 {
			return t.createTreeKeyPosition(node, last, values[end], end, CompareGt)
		}
	}
	//节点范围(left<key<right)，选取以left节点位置(一个节点key对齐方式以左侧方式)
	//对齐方式针对中间key排列，实际最左和最右都为开区间
	for front < end {
		mid := (front+end)/2
		current := keys[mid]
		if bytes.Compare(key, current) == 0 {
			return t.createTreeKeyPosition(node, current, values[mid], mid, CompareEq)
		}else if bytes.Compare(key, current) == 1 {
			next := keys[mid+1]
			if bytes.Compare(key, next) == -1 {
				return t.createTreeKeyPosition(node, current, values[mid], mid, CompareGt)
			}
			front = mid + 1
		}else{
			prevIndex := mid - 1
			prev := keys[prevIndex]
			if bytes.Compare(key, prev) == 1 {
				return t.createTreeKeyPosition(node, prev, values[prevIndex], prevIndex, CompareGt)
			}
			end = mid - 1
		}
	}
	return keyPosition,nil
}

func (t *DbManager) moveKey(node *TreeNode, position Position, compare CompareType, key []byte, value []byte) error {
	if compare == CompareEq { //变更
		node.Keys[position] = key
		node.Values[position] = value
	}else{//插入
		keyNum := int16(len(node.Keys))
		if position+1 == keyNum {//插入到最右边
			node.Keys = append(node.Keys, key)
			node.Values = append(node.Values, value)
		}else{//插入中间，需要移动右边元素
			move := position//默认移动右边元素，即插入在右边，当前元素无需移动
			if compare == CompareLt {//插入到左边，当前元素需要右移动，填充到当前位置
				move--
			}else if compare == CompareGt {//插入到右边，填充到右边位置
				position++
			}
			//动态增加数组长度
			node.Keys = append(node.Keys, nil)
			node.Values = append(node.Values, nil)
			//从move位置之后所有元素往右移动
			for i:=keyNum;i>move;i-- {
				node.Keys[i] = node.Keys[i-1]
				node.Values[i] = node.Values[i-1]

			}
			node.Keys[position] = key
			node.Values[position] = value
		}
	}
	return nil
}

func (t *DbManager) balanceTree(node *TreeNode, cache *TreeNodeCache) error {
	isMax,err := t.nodeSizeIsMax(node, cache); if err != nil {
		return err
	}
	if isMax {//单个节点容量已满，做分裂
		parentNode,err := t.splitNode(node, cache); if err != nil {
			return err
		}
		if parentNode != nil {
			err = t.balanceTree(parentNode, cache); if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *DbManager) splitNode(node *TreeNode, cache *TreeNodeCache) (*TreeNode,error) {
	position := node.Position
	keyNum := int16(len(node.Keys))
	mid := keyNum/2
	leftKeys := node.Keys[:mid]
	leftValues := node.Values[:mid]
	rightKeys := node.Keys[mid:]
	rightValues := node.Values[mid:]
	node.Keys = leftKeys
	node.Values = leftValues
	rightNode := t.createNode(node.Type, node.Parent, position + 1, rightKeys, rightValues, cache)
	parentRightKey := rightKeys[0]
	parentRightValue,err := t.formatValue(rightNode.Pointer, NodeValueTypePointer); if err != nil {
		return nil,fmt.Errorf("split node error `%s`", err)
	}
	if cache.head.Height == 1 || node.Type == NodeTypeRoot {//当前节点为根节点，需要创建新根节点
		parentLeftValue,err := t.formatValue(node.Pointer, NodeValueTypePointer); if err != nil {
			return nil,fmt.Errorf("split node error `%s`", err)
		}
		rootKeys := [][]byte{leftKeys[0], parentRightKey}
		rootValues := [][]byte{parentLeftValue,	parentRightValue}
		rootNode := t.createNode(NodeTypeRoot,0, 0, rootKeys, rootValues, cache)
		nodeType := NodeTypeLeaf
		if cache.head.Height > 1 {
			nodeType = NodeTypeChild
		}
		node.Type = nodeType
		rightNode.Type = nodeType
		node.Parent = rootNode.Pointer
		rightNode.Parent = rootNode.Pointer
		node.Position = Position(0)
		rightNode.Position = Position(1)
		cache.head.Root = rootNode.Pointer
		cache.head.Height = cache.head.Height + 1
		return nil,nil
	}else{//右边节点的第一个key需要插入到父级中(需要做移动)
		parentNode,err := t.getNode(node.Parent, cache)
		if err != nil {
			return nil,fmt.Errorf("node `%d` cache read miss `%s`", node.Parent, err.Error())
		}
		//在父级中key往原位置右边插入一个节点key
		err = t.moveKey(parentNode, position, CompareGt, parentRightKey, parentRightValue); if err != nil {
			return nil,err
		}
		return parentNode,nil
	}
}

func (t *DbManager) convertValue(value interface{}, insertType InsertType, keyPosition *TreeKeyPosition, cache *TreeNodeCache) ([]byte,error) {
	// TODO value需要判定是否符合原值类型
	valueType := NodeValueTypeData
	if keyPosition.compare == CompareEq {//存在情况需要判定插入类型
		if insertType == InsertTypeDefault {//验证唯一性
			return nil,fmt.Errorf("key is already")
		}else if insertType == InsertTypeReplace {//替换原有值
			valueType = keyPosition.valueType//类型必须符合原类型
		}else if insertType == InsertTypeAppend {//在原值追加
			valueType = NodeValueTypeCollection//类型变更为集合
			var collection [][]byte
			if keyPosition.valueType != NodeValueTypeCollection {//原值需要和当前值组合
				collection = append(collection, keyPosition.value.([]byte))
				collection = append(collection, value.([]byte))
			}else{//原值为集合类型直接追加
				collection := keyPosition.value.([][]byte)
				collection = append(collection, value.([]byte))
			}
			value = collection
		}else{//类型错误
			return nil,fmt.Errorf("insert type error")
		}
	}
	//不存在直接插入
	convertValue,err := t.formatValue(value, valueType); if err != nil {
		return nil,err
	}
	return convertValue,nil
}

func (t *DbManager) treeIsNull(head *TreeHead) bool {
	if head == nil || head.Root == 0 || head.Height == 0 {
		return true
	}
	return false
}

func (t *DbManager) Search(table string, column string, key []byte) (*TreeNode,error) {
	cache := t.createTreeNodeCache(nil,false)
	head,err := t.getHead(table, column, cache); if err != nil {
		return nil,err
	}
	if t.treeIsNull(head) {
		return nil,fmt.Errorf("tree is null")
	}
	keyPosition,err := t.findPosition(key, cache); if err != nil {
		return nil,err
	}
	if keyPosition.compare != CompareEq {
		return nil,fmt.Errorf("key `%s` not found", key)
	}
	return keyPosition.node,nil
}

/*
	插入key到树中
*/
func (t *DbManager) Insert(table string, column string, key []byte, value interface{}, insertType InsertType) error {
	cache := t.createTreeNodeCache(nil,true)
	head,err := t.getHead(table, column, cache); if err != nil {
		return err
	}
	if t.treeIsNull(head) {
		root := t.createNode(NodeTypeLeaf,0,0, [][]byte{}, [][]byte{}, cache)
		head.Root = root.Pointer
		head.Height = 1
	}
	keyPosition,err := t.findPosition(key, cache); if err != nil {
		return err
	}
	//处理叶子节点多种数据类型
	convertValue,err := t.convertValue(value, insertType, keyPosition, cache); if err != nil {
		return err
	}
	//叶子节点写入key
	//匹配到key或最近的key，写入key(可能产生移动)，节点分裂，节点移动(具体看当前节点容量)
	err = t.moveKey(keyPosition.node, keyPosition.position, keyPosition.compare, key, convertValue); if err != nil {
		return err
	}
	//平衡树结构
	err = t.balanceTree(keyPosition.node, cache); if err != nil {
		return err
	}
	//保存所有需要变更的节点
	err = t.putNode(cache); if err != nil {
		return err
	}
	return nil
}

func (t *DbManager) Print(table string, column string) ([]string,error) {
	cache := t.createTreeNodeCache(nil,false)
	head,err := t.getHead(table, column, cache); if err != nil {
		return nil,err
	}
	if t.treeIsNull(head) {
		return nil,fmt.Errorf("tree is null")
	}
	node,err := t.getNode(head.Root, cache); if err != nil {
		return nil,err
	}

	str := make([]string,head.Height+1)
	return t.printRecursionNode(*node, 0, str, cache)
}

func (t *DbManager) printRecursionNode(node TreeNode, height int8, str []string, cache *TreeNodeCache) ([]string,error) {
	space := ","
	for i,k := range node.Keys {
		fmt.Println(k)
		printKey := fmt.Sprintf("%v%s", k, space)
		str[height] = str[height] + printKey
		value := node.Values[i]
		if node.Type == NodeTypeLeaf {
			printValue := fmt.Sprintf("%v%s", value, space)
			str[height+1] = str[height+1] + printValue
		}else{
			pointer,_,err := t.parseValue(value); if err != nil {
				return nil,err
			}
			childNode,err := t.getNode(t.toPointer(pointer), cache); if err != nil {
				return nil,err
			}
			height++
			return t.printRecursionNode(*childNode, height, str, cache)
		}
	}
	return str,nil
}