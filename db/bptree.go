package db

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const (
	NODE_POINTER_SIZE = 4 //节点指针，pointer为int32类型
	MAX_NODE_SIZE = 1024*4 //节点最大容量4KB
	MAX_KEY_NUM = 1000 //节点最大key数量，position为int16类型
	MAX_NODE_NUM = 100000 //树最大节点数量
	MAX_TREE_HEIGHT = 4 //树最大阶数

	NODE_SPLIT_RULE = 1 //节点分裂规则：1为关键字个数验证
	MAX_NODE_KEY_NUM = 3
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
	NodeValueTypePointer
	NodeValueTypeCollection
)

type InsertType int8
const (
	InsertTypeDefault InsertType = iota
	InsertTypeReplace
	InsertTypeAppend
)

type TreeNodePosition struct {
	Node *TreeNode
	Parent *TreeNodePosition//父级指针
	Position Position//在父级的位置
}

//节点key所在位置
type TreeKeyPosition struct {
	Node *TreeNode
	Key []byte
	Value interface{}
	ValueType NodeValueType
	Position Position
	Compare CompareType
}

//缓存树信息、已读到的节点集合和待写入的节点集合,减少io次数
type TreeNodeCache struct {
	Head *TreeHead
	IsWrite bool
	Read map[Pointer]*TreeNodePosition
	Write map[Pointer]*TreeNodePosition
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
	if node.Type == NodeTypeRoot || node.Type == NodeTypeChild {
		if valueType != NodeValueTypePointer {
			return TreeKeyPosition{},fmt.Errorf("node value type must is NodeValueTypePointer")
		}
	}
	return TreeKeyPosition{node,key,convertValue,valueType,position,compare},nil
}

func (t *DbManager) getHead(table string, column string, cache *TreeNodeCache) (*TreeHead,error) {
	if cache.Head != nil {
		return cache.Head,nil
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
	cache.Head = head
	return head,nil
}

func (t *DbManager) getNodePosition(pointer Pointer, parent *TreeNodePosition, position Position, cache *TreeNodeCache) (*TreeNodePosition,error) {
	nodePosition,ok := cache.Read[pointer]
	if ok {
		return nodePosition,nil
	}
	nodeBytes,err := t.getBPTreeNode(cache.Head.Table, cache.Head.Column, t.Int64ToString(int64(pointer))); if err != nil {
		return nil,err
	}
	if nodeBytes == nil || len(nodeBytes) == 0 {
		return nil,fmt.Errorf("node `%d` not found", pointer)
	}else{
		node := &TreeNode{}
		if err := json.Unmarshal(nodeBytes, node); err != nil {
			return nil,err
		}
		nodePosition = &TreeNodePosition{node,parent,position}
		if cache.IsWrite {
			cache.Read[pointer] = nodePosition
		}
	}
	return nodePosition,nil
}

func (t *DbManager) putNode(cache *TreeNodeCache) error {
	table := cache.Head.Table
	column := cache.Head.Column
	for pointer,nodePosition := range cache.Write {
		//fmt.Println(nodePosition.Node.Keys)
		//fmt.Println(nodePosition.Node.Values)
		//fmt.Println(t.ConvertJsonString(*nodePosition.Node))
		nodeBytes, err := t.ConvertJsonBytes(*nodePosition.Node); if err != nil {
			return err
		}
		if err := t.putBPTreeNode(table, column, t.Int64ToString(int64(pointer)), nodeBytes); err != nil {
			return err
		}
	}
	headBytes, err := t.ConvertJsonBytes(*cache.Head); if err != nil {
		return err
	}
	if err := t.putBPTreeHead(table, column, headBytes); err != nil {
		return err
	}
	return nil
}

func (t *DbManager) createNodePosition(nodeType NodeType, parent *TreeNodePosition, position Position, keys [][]byte, values [][]byte, cache *TreeNodeCache) *TreeNodePosition {
	pointer := cache.Head.NodeOrder + 1
	node := &TreeNode{pointer,nodeType,keys,values}
	nodePosition := &TreeNodePosition{node,parent,position}
	cache.Read[pointer] = nodePosition
	cache.Write[pointer] = nodePosition
	cache.Head.NodeOrder = pointer
	cache.Head.NodeNum++
	if nodeType == NodeTypeRoot {
		cache.Head.Root = pointer
		cache.Head.Height++
		cache.Head.KeyNum = cache.Head.KeyNum + int64(len(keys))
	}
	return nodePosition
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
		if len(node.Keys) > MAX_NODE_KEY_NUM {
			isMax = true
		}
	}else{
		nodeSize,err := t.getNodeSize(node); if err != nil {
			return false,err
		}
		if nodeSize > MAX_NODE_SIZE {
			isMax = true
		}
	}
	if isMax { //单个节点容量已满，做分裂
		if cache.Head.Height == MAX_TREE_HEIGHT && node.Type == NodeTypeRoot {//根节点已满，无法新增
			return false,fmt.Errorf("root is full")
		}
		return true,nil
	}
	return false,nil
}

func (t *DbManager) createTreeNodeCache(head *TreeHead, isWrite bool) *TreeNodeCache {
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
	查询key所在叶子节点位置
	从根开始递归查找(用二分法比较key大小)，直到找到叶子节点
	返回最近节点、最近节点key、最近key位置、比较结果，比较结果返回三种情况(等于：找到key位置，大于：右边，小于：左边)
 */
func (t *DbManager) findPosition(key []byte, cache *TreeNodeCache) (*TreeNodePosition,*TreeKeyPosition,error) {
	var keyPosition *TreeKeyPosition
	var nodePosition *TreeNodePosition
	head := cache.Head

	rootNodePosition,err := t.getNodePosition(head.Root,nil,-1, cache); if err != nil {
		return nil,nil,err
	}

	if head.Height == 1 {//树阶数为1，叶子节点为根节点
		position,err := t.binarySearch(rootNodePosition.Node, key); if err != nil {
			return nil,nil,err
		}
		keyPosition = &position
		nodePosition = rootNodePosition
	}else{//从根遍历查找到最底层叶子节点位置
		nodePosition,keyPosition,err = t.recursionNode(rootNodePosition, key, cache); if err != nil {
			return nil,nil,err
		}
	}
	//写入查找时需要缓存查找的节点和待写入的叶子节点
	if cache.IsWrite {
		cache.Write[nodePosition.Node.Pointer] = nodePosition
	}
	return nodePosition,keyPosition,nil
}

func (t *DbManager) recursionNode(nodePosition *TreeNodePosition, key []byte, cache *TreeNodeCache) (*TreeNodePosition,*TreeKeyPosition,error) {
	if len(nodePosition.Node.Keys) == 0 {//节点为空，无法递归
		return nil,nil,fmt.Errorf("recursionNode node `%d` is null", nodePosition.Node.Pointer)
	}
	keyPosition,err := t.binarySearch(nodePosition.Node, key); if err != nil {
		return nil,nil,err
	}
	if nodePosition.Node.Type == NodeTypeLeaf {//递归到最底层叶子节点
		return nodePosition,&keyPosition,nil
	}else{//递归查找下级节点
		lower,err := t.getNodePosition(t.toPointer(keyPosition.Value), nodePosition, keyPosition.Position, cache); if err != nil {
			return nil,nil,err
		}
		return t.recursionNode(lower, key, cache)
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

/*
	节点内部key变动
*/
func (t *DbManager) moveKey(nodePosition *TreeNodePosition, position Position, compare CompareType, key []byte, value []byte, cache *TreeNodeCache) error {
	node := nodePosition.Node
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
		cache.Head.KeyNum++
	}
	//缓存待写入节点
	cache.Write[node.Pointer] = nodePosition
	return nil
}

/*
	调整树，节点容量超出需要递归分裂
*/
func (t *DbManager) balanceTree(nodePosition *TreeNodePosition, cache *TreeNodeCache) error {
	isMax,err := t.nodeSizeIsMax(nodePosition.Node, cache); if err != nil {
		return err
	}
	if isMax {//单个节点容量已满，做分裂
		parent,err := t.splitNode(nodePosition, cache); if err != nil {
			return err
		}
		if parent != nil {
			err = t.balanceTree(parent, cache); if err != nil {
				return err
			}
		}
	}
	return nil
}

/*
	节点分裂，从叶子节点往上级分裂
*/
func (t *DbManager) splitNode(nodePosition *TreeNodePosition, cache *TreeNodeCache) (*TreeNodePosition,error) {
	position := nodePosition.Position
	node := nodePosition.Node
	keyNum := int16(len(node.Keys))
	mid := keyNum/2
	leftKeys := node.Keys[:mid]
	leftValues := node.Values[:mid]
	rightKeys := node.Keys[mid:]
	rightValues := node.Values[mid:]
	node.Keys = leftKeys
	node.Values = leftValues
	rightNodePosition := t.createNodePosition(node.Type, nodePosition.Parent, position + 1, rightKeys, rightValues, cache)
	parentRightKey := rightKeys[0]
	parentRightValue,err := t.formatValue(rightNodePosition.Node.Pointer, NodeValueTypePointer); if err != nil {
		return nil,fmt.Errorf("split node error `%s`", err)
	}
	if cache.Head.Height == 1 || node.Type == NodeTypeRoot {//当前节点为根节点，需要创建新根节点
		parentLeftValue,err := t.formatValue(node.Pointer, NodeValueTypePointer); if err != nil {
			return nil,fmt.Errorf("split node error `%s`", err)
		}
		rootKeys := [][]byte{leftKeys[0], parentRightKey}
		rootValues := [][]byte{parentLeftValue,	parentRightValue}
		rootNodePosition := t.createNodePosition(NodeTypeRoot,nil, 0, rootKeys, rootValues, cache)
		nodePosition.Parent = rootNodePosition
		rightNodePosition.Parent = rootNodePosition
		nodePosition.Position = Position(0)
		rightNodePosition.Position = Position(1)
		if node.Type == NodeTypeRoot {
			node.Type = NodeTypeChild
			rightNodePosition.Node.Type = NodeTypeChild
		}
		return nil,nil
	}else{//右边节点的第一个key需要插入到父级中(需要做移动)
		if nodePosition.Parent == nil {
			return nil,fmt.Errorf("node `%d` parent cache read miss `%s`", nodePosition.Node.Pointer, err.Error())
		}
		//在父级中key往原位置右边插入一个节点key
		err = t.moveKey(nodePosition.Parent, position, CompareGt, parentRightKey, parentRightValue, cache); if err != nil {
			return nil,err
		}
		return nodePosition.Parent,nil
	}
}

func (t *DbManager) convertValue(value interface{}, insertType InsertType, keyPosition *TreeKeyPosition, cache *TreeNodeCache) ([]byte,error) {
	// TODO value需要判定是否符合原值类型
	valueType := NodeValueTypeData
	if keyPosition.Compare == CompareEq {//存在情况需要判定插入类型
		if insertType == InsertTypeDefault {//验证唯一性
			return nil,fmt.Errorf("key is already")
		}else if insertType == InsertTypeReplace {//替换原有值
			valueType = keyPosition.ValueType//类型必须符合原类型
		}else if insertType == InsertTypeAppend {//在原值追加
			valueType = NodeValueTypeCollection//类型变更为集合
			var collection [][]byte
			if keyPosition.ValueType != NodeValueTypeCollection {//原值需要和当前值组合
				collection = append(collection, keyPosition.Value.([]byte))
				collection = append(collection, value.([]byte))
			}else{//原值为集合类型直接追加
				collection := keyPosition.Value.([][]byte)
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
	_,keyPosition,err := t.findPosition(key, cache); if err != nil {
		return nil,err
	}
	if keyPosition.Compare == CompareEq {
		return keyPosition.Node,nil
	}
	return nil,nil
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
		rootPosition := t.createNodePosition(NodeTypeLeaf,nil,0, [][]byte{}, [][]byte{}, cache)
		head.Root = rootPosition.Node.Pointer
		head.Height = 1
	}
	nodePosition,keyPosition,err := t.findPosition(key, cache); if err != nil {
		return err
	}
	//处理叶子节点多种数据类型
	convertValue,err := t.convertValue(value, insertType, keyPosition, cache); if err != nil {
		return err
	}
	//叶子节点写入key
	//匹配到key或最近的key，写入key(可能产生移动)，节点分裂，节点移动(具体看当前节点容量)
	err = t.moveKey(nodePosition, keyPosition.Position, keyPosition.Compare, key, convertValue, cache); if err != nil {
		return err
	}
	//平衡树结构
	err = t.balanceTree(nodePosition, cache); if err != nil {
		return err
	}
	//保存所有需要变更的节点
	err = t.putNode(cache); if err != nil {
		return err
	}
	return nil
}

func (t *DbManager) Print(table string, column string) error {
	cache := t.createTreeNodeCache(nil,false)
	head,err := t.getHead(table, column, cache); if err != nil {
		return err
	}
	if t.treeIsNull(head) {
		return fmt.Errorf("tree is null")
	}
	nodePosition,err := t.getNodePosition(head.Root,nil,-1, cache); if err != nil {
		return err
	}

	str := make([]string,head.Height*2)
	str,err = t.printRecursionNode(nodePosition, -1, str, cache); if err != nil {
		return err
	}
	fmt.Println(t.ConvertJsonString(*head))
	for _,v := range str {
		fmt.Println(v)
	}
	return nil
}

func (t *DbManager) printRecursionNode(nodePosition *TreeNodePosition, height int8, str []string, cache *TreeNodeCache) ([]string,error) {
	space := ""
	height = height + 2
	node := nodePosition.Node
	for i,k := range node.Keys {
		value := node.Values[i]
		printKey := ""
		pointer := Pointer(0)
		if node.Type != NodeTypeLeaf {
			pi,_,err := t.parseValue(value); if err != nil {
				return nil,err
			}
			pointer = t.toPointer(pi)
			printKey = fmt.Sprintf("(%v->&%d) %s", k, pointer, space)
		}else{
			printKey = fmt.Sprintf("(%v:%v) %s", k, value, space)
		}
		if i == 0 {
			printKey = fmt.Sprintf("&%d【 " + printKey, node.Pointer)
		}
		if i==len(node.Keys)-1 {
			printKey = printKey + "】     "
		}
		str[height-1] = str[height-1] + printKey
		//printValue := fmt.Sprintf("%v%s", value, space)
		//str[height] = str[height] + printValue
		if pointer > 0 {
			childNode,err := t.getNodePosition(pointer, nodePosition, -1, cache); if err != nil {
				return nil,err
			}
			str,err = t.printRecursionNode(childNode, height, str, cache); if err != nil {
				return nil,err
			}
		}
	}
	return str,nil
}