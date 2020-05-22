package bptree

import (
	"bytes"
	"fmt"
	"github.com/bidpoc/database-fabric-cc/db"
	"github.com/bidpoc/database-fabric-cc/db/index/tree"
	"github.com/bidpoc/database-fabric-cc/db/util"
)

type Position = int16 //关键字位置，最大值32768(一个节点关键字数量不超过1000)

//节点所在位置
type TreeNodePosition struct {
	Pointer  tree.Pointer           //节点指针
	Node     *tree.TreeNode         //节点数据
	Parent   *TreeNodePosition 		//父级指针
	Prev   *TreeNodePosition 		//左兄弟指针
	Next   *TreeNodePosition 		//右兄弟级指针
	Position Position          		//在父级的位置
}

//节点key所在位置
type TreeKeyPosition struct {
	NodePosition *TreeNodePosition //节点位置
	Position     Position          //key位置
	Compare      tree.CompareType       //比较类型
}

//节点key所在位置和数据
type TreeKeyData struct {
	KeyPosition TreeKeyPosition //key位置
	Data        db.KV       //匹配到的KV
}

//缓存树信息、已读到的节点集合和待写入的节点集合,减少io次数
type TreeNodeCache struct {
	Head    *tree.TreeHead                     //树头
	IsWrite bool                          //写入需要缓存读写
	Read    map[tree.Pointer]*TreeNodePosition //缓存读
	Write   map[tree.Pointer]*TreeNodePosition //缓存写
}

//节点值列表key和value
type TreeNodeKV struct {
	Key   []byte
	Value []byte
}

//树分裂结构
type TreeSplit struct {
	Child *TreeKeyPosition //记录分裂的子节点(分裂是从子往父，故传递到父节点需要缓存子节点数据)
	Current *TreeKeyPosition //当前节点数据
	UpdateKV *TreeNodeKV //待更新值
	InsertKV *TreeNodeKV //插入值
	Cache *TreeNodeCache
}

func SplitMid(num Position) Position {
	n := num%2
	mid := num / 2
	if n > 0 {
		mid++
	}
	return mid
}

func GetNodeSize(node *tree.TreeNode) (int, error) {
	nodeBytes, err := util.ConvertJsonBytes(*node)
	if err != nil {
		return 0, err
	}
	nodeSize := len(nodeBytes) + tree.NODE_NAME_SIZE
	return nodeSize, nil
}

//////////////////////////////// TreeNodeCache Function ////////////////////////////////
func (cache *TreeNodeCache) createNode(nodePosition *TreeNodePosition) {
	cache.setRead(nodePosition)
	cache.setWrite(nodePosition)
	cache.Head.NodeOrder = nodePosition.Pointer
	cache.Head.NodeNum++
}

func (cache *TreeNodeCache) keyNumIncrement() {
	cache.Head.KeyNum++
}

func (cache *TreeNodeCache) setRead(nodePosition *TreeNodePosition) {
	cache.Read[nodePosition.Pointer] = nodePosition
}

/*
	所有变更节点需要写入缓存，最后统一由缓存写入磁盘
	每次写入缓存计算叶子头节点或叶子尾节点
*/
func (cache *TreeNodeCache) setWrite(nodePosition *TreeNodePosition) {
	cache.Write[nodePosition.Pointer] = nodePosition
	if nodePosition.Node.Type == tree.NodeTypeLeaf {//记录叶子头节点和叶子尾节点
		if cache.Head.FirstLeaf == 0 {
			if nodePosition.Prev == nil {
				cache.Head.FirstLeaf = nodePosition.Pointer
			}
		}else if nodePosition.Pointer == cache.Head.FirstLeaf {
			if nodePosition.Prev != nil {
				cache.Head.FirstLeaf = nodePosition.Prev.Pointer
			}
		}
		if cache.Head.LastLeaf == 0 {
			if nodePosition.Next == nil {
				cache.Head.LastLeaf = nodePosition.Pointer
			}
		}else if nodePosition.Pointer == cache.Head.LastLeaf {
			if nodePosition.Next != nil {
				cache.Head.LastLeaf = nodePosition.Next.Pointer
			}
		}
	}
}

//////////////////////////////// TreeNodePosition Function ////////////////////////////////
func (nodePosition *TreeNodePosition) setPrev(prev *TreeNodePosition) {
	nodePosition.Prev = prev
	nodePosition.Node.Prev = prev.Pointer
}

func (nodePosition *TreeNodePosition) setNext(next *TreeNodePosition) {
	nodePosition.Next = next
	nodePosition.Node.Next = next.Pointer
}

func (nodePosition *TreeNodePosition) createTreeKeyData(key []byte, value []byte, position Position, compare tree.CompareType) (TreeKeyData, error) {
	return TreeKeyData{TreeKeyPosition{nodePosition,position,compare},db.KV{Key:key,Value:value}}, nil
}

/*
	节点内部key二分查找
	返回最近节点key、最近key位置、比较结果，比较结果返回三种情况(等于：找到key位置，大于：右边，小于：左边)
*/
func (nodePosition *TreeNodePosition) binarySearch(key []byte) (TreeKeyData, error) {
	keyData := TreeKeyData{}
	//默认未找到情况位置大于-1位置
	keyData.KeyPosition = TreeKeyPosition{nodePosition, -1, tree.CompareGt}
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
		return nodePosition.createTreeKeyData(first, values[front], front, tree.CompareEq)
	}
	if bytes.Compare(key, first) == -1 {
		return nodePosition.createTreeKeyData(first, values[front], front, tree.CompareLt)
	}
	if front == end {
		if bytes.Compare(key, first) == 1 {
			return nodePosition.createTreeKeyData(first, values[front], front, tree.CompareGt)
		}
	} else {
		last := keys[end]
		if bytes.Compare(key, last) == 0 {
			return nodePosition.createTreeKeyData(last, values[end], end, tree.CompareEq)
		} else if bytes.Compare(key, last) == 1 {
			return nodePosition.createTreeKeyData(last, values[end], end, tree.CompareGt)
		}
	}
	//节点范围(left<key<right)，选取以left节点位置(一个节点key对齐方式以左侧方式)
	//对齐方式针对中间key排列，实际最左和最右都为开区间
	for front <= end {
		mid := (front + end) / 2
		current := keys[mid]
		if bytes.Compare(key, current) == 0 {
			return nodePosition.createTreeKeyData(current, values[mid], mid, tree.CompareEq)
		} else if bytes.Compare(key, current) == 1 {
			next := keys[mid+1]
			if bytes.Compare(key, next) == -1 {
				return nodePosition.createTreeKeyData(current, values[mid], mid, tree.CompareGt)
			}
			front = mid + 1
		} else {
			prevIndex := mid - 1
			prev := keys[prevIndex]
			if bytes.Compare(key, prev) == 1 {
				return nodePosition.createTreeKeyData(prev, values[prevIndex], prevIndex, tree.CompareGt)
			}
			end = mid - 1
		}
	}
	return keyData, nil
}

func TreeIsNull(head *tree.TreeHead) bool {
	if head == nil || head.Root == 0 || head.Height == 0 {
		return true
	}
	return false
}

///////////////////// Private Function ////////////////////
func createNodePosition(nodeType tree.NodeType, parent *TreeNodePosition, position Position, keys [][]byte, values [][]byte, cache *TreeNodeCache) (*TreeNodePosition, error) {
	if !cache.IsWrite {
		return nil, fmt.Errorf("createNodePosition error, cache must set isWrite is true")
	}
	pointer := cache.Head.NodeOrder + 1
	node := &tree.TreeNode{Type:nodeType,Prev:tree.Pointer(0),Next:tree.Pointer(0),Keys:keys,Values:values}
	nodePosition := &TreeNodePosition{pointer,node,parent,nil,nil,position}
	cache.createNode(nodePosition)
	return nodePosition, nil
}

func createTreeNodeCache(head *tree.TreeHead, isWrite bool) (*TreeNodeCache,error) {
	if head == nil {
		return nil,fmt.Errorf("tree head is null")
	}
	cache := &TreeNodeCache{}
	cache.Head = head
	cache.IsWrite = isWrite
	if isWrite {
		cache.Read = map[tree.Pointer]*TreeNodePosition{}
		cache.Write = map[tree.Pointer]*TreeNodePosition{}
	}
	return cache,nil
}

//////////////////////////////// TreeSplit Function ////////////////////////////////

/**
节点分裂规则(计算待插入key是否有空间)：1、key数量>=2 2、key数量>=最大数量 3、节点大小+插入key大小>最大容量
*/
func (split *TreeSplit) GetIsSplit() (bool, error) {
	node := split.Current.NodePosition.Node
	kv := split.InsertKV
	height := split.Cache.Head.Height
	keyNum := len(node.Keys)
	if keyNum == 0 {//key数量必须2个以上才能分裂
		return false,nil
	}
	isSplit := false
	if tree.NODE_SPLIT_RULE == 1 {
		if keyNum >= tree.MAX_NODE_KEY_NUM {//当前数量已满，无法插入
			isSplit = true
		}
	} else {
		nodeSize, err := GetNodeSize(node)
		if err != nil {
			return false, err
		}
		addSize := len(kv.Key) + len(kv.Value) + tree.NODE_POINTER_SIZE*2
		if nodeSize+addSize > tree.MAX_NODE_SIZE {
			isSplit = true
		}
	}
	if isSplit { //单个节点容量已满，做分裂
		if height == tree.MAX_TREE_HEIGHT && node.Type == tree.NodeTypeRoot { //根节点已满，无法新增
			return false, fmt.Errorf("root is full")
		}
	}
	return isSplit, nil
}

/*
	调整树，节点容量超出需要递归分裂
*/
func (split *TreeSplit) balanceTree(isSplit bool) error {
	var err error
	if !isSplit {
		isSplit, err = split.GetIsSplit()
		if err != nil {
			return err
		}
	}

	if isSplit { //节点分裂，节点移动(具体看当前节点容量)
		err := split.splitNode()
		if err != nil {
			return err
		}
		if split.Current != nil {
			err = split.balanceTree(false)
			if err != nil {
				return err
			}
		}
	} else { //插入key
		//匹配到key或最近的key，写入key(可能产生移动)
		err = split.moveKey()
		if err != nil {
			return err
		}
	}
	return nil
}

func (split *TreeSplit) splitToFirstNode() error {
	keyPosition := split.Current
	kv := split.InsertKV
	nodePosition := keyPosition.NodePosition
	node := nodePosition.Node
	firstNodePosition, err := createNodePosition(node.Type, nodePosition.Parent, nodePosition.Position-1, [][]byte{kv.Key}, [][]byte{kv.Value}, split.Cache)
	if err != nil {
		return  err
	}
	parentV := tree.PointerToBytes(firstNodePosition.Pointer)
	split.Child = keyPosition
	split.Current = &TreeKeyPosition{nodePosition.Parent,nodePosition.Position,keyPosition.Compare}
	split.UpdateKV = nil
	split.InsertKV = &TreeNodeKV{kv.Key, parentV}

	//叶子节点通过左右指针建立双向链表
	if node.Type == tree.NodeTypeLeaf {
		firstNodePosition.setNext(nodePosition)
		nodePosition.setPrev(firstNodePosition)
		split.Cache.setWrite(nodePosition)
	}

	return nil
}

func (split *TreeSplit) splitToRightNode(rightKeys [][]byte, rightValues [][]byte) (*TreeNodePosition,error) {
	keyPosition := split.Current
	nodePosition := keyPosition.NodePosition
	node := nodePosition.Node
	position := nodePosition.Position
	rightNodePosition, err := createNodePosition(node.Type, nodePosition.Parent, position+1, rightKeys, rightValues, split.Cache)
	if err != nil {
		return nil,err
	}

	//叶子节点通过左右指针建立双向链表
	if node.Type == tree.NodeTypeLeaf {
		rightNodePosition.setPrev(nodePosition)
		if nodePosition.Next != nil {
			rightNodePosition.setNext(nodePosition.Next)
			nodePosition.Next.setPrev(rightNodePosition)
			split.Cache.setWrite(nodePosition.Next)
		}
		nodePosition.setNext(rightNodePosition)
		split.Cache.setWrite(nodePosition)
	}

	return rightNodePosition,nil
}

func (split *TreeSplit) splitToRootNode(rightNodePosition *TreeNodePosition) error {
	keyPosition := split.Current
	nodePosition := keyPosition.NodePosition
	node := nodePosition.Node
	parentLeftKey := node.Keys[0]
	parentLeftValue := tree.PointerToBytes(nodePosition.Pointer)
	parentRightKey := rightNodePosition.Node.Keys[0]
	parentRightValue := tree.PointerToBytes(rightNodePosition.Pointer)
	rootKeys := [][]byte{parentLeftKey, parentRightKey}
	rootValues := [][]byte{parentLeftValue, parentRightValue}
	rootNodePosition, err := createNodePosition(tree.NodeTypeRoot,nil, 0, rootKeys, rootValues, split.Cache)
	if err != nil {
		return err
	}
	nodePosition.Position = Position(0)
	nodePosition.Parent = rootNodePosition
	rightNodePosition.Position = Position(1)
	rightNodePosition.Parent = rootNodePosition
	if node.Type == tree.NodeTypeRoot {
		node.Type = tree.NodeTypeChild
		rightNodePosition.Node.Type = tree.NodeTypeChild
	}
	split.Cache.Head.Root = rootNodePosition.Pointer
	split.Cache.Head.Height++
	split.Cache.Head.KeyNum = split.Cache.Head.KeyNum + 2
	split.Cache.setWrite(nodePosition)
	split.Child = nil
	split.Current = nil
	split.UpdateKV = nil
	split.InsertKV = nil
	return nil
}

func (split *TreeSplit) splitToKey(rightNodePosition *TreeNodePosition, isBinary bool) error {
	keyPosition := split.Current
	nodePosition := keyPosition.NodePosition
	node := nodePosition.Node
	position := nodePosition.Position
	if nodePosition.Parent == nil {
		return fmt.Errorf("node `%d` parent cache read miss", nodePosition.Pointer)
	}
	parentLeftKey := node.Keys[0]
	parentLeftValue := tree.PointerToBytes(nodePosition.Pointer)
	parentRightKey := rightNodePosition.Node.Keys[0]
	parentRightValue := tree.PointerToBytes(rightNodePosition.Pointer)

	split.Child = keyPosition
	split.Current = &TreeKeyPosition{nodePosition.Parent, position, tree.CompareGt}
	if isBinary {//二分法分裂原key需要更新为左边节点的新key
		split.UpdateKV = &TreeNodeKV{parentLeftKey, parentLeftValue}
	}else{
		split.UpdateKV = nil
	}
	split.InsertKV = &TreeNodeKV{parentRightKey, parentRightValue}
	return nil
}

/*
	节点分裂，从叶子节点往上级分裂
*/
func (split *TreeSplit) splitNode() error {
	childKeyPosition := split.Child
	keyPosition := split.Current
	kv := split.InsertKV
	nodePosition := keyPosition.NodePosition
	node := nodePosition.Node
	compare := keyPosition.Compare
	isRoot := split.Cache.Head.Height == 1 || node.Type == tree.NodeTypeRoot
	if !isRoot && compare == tree.CompareLt && keyPosition.Position == 0 { //最左边,如果是根节点，必须按1/2分裂
		if childKeyPosition != nil { //对应的子节点位置修改为0
			childKeyPosition.Position = 0
		}
		split.Cache.keyNumIncrement()
		return split.splitToFirstNode()
	} else { //排序树和默认树都往右边分裂出新节点
		isOrder := split.Cache.Head.Type == tree.TreeTypeAsc || split.Cache.Head.Type == tree.TreeTypeDesc
		isBinary := false
		var rightKeys [][]byte
		var rightValues [][]byte
		if isOrder && compare == tree.CompareGt && keyPosition.Position+1 == int16(len(node.Keys)) { //只有排序树到最右边开始分裂
			if childKeyPosition != nil { //对应的子节点位置修改为0
				childKeyPosition.Position = 0
			}
			rightKeys = [][]byte{kv.Key}
			rightValues = [][]byte{kv.Value}
			split.Cache.keyNumIncrement()
		} else { //其他默认按1/2分裂
			err := split.moveKey(); if err != nil {
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
		rightNodePosition,err := split.splitToRightNode(rightKeys, rightValues); if err != nil {
			return  err
		}
		if isRoot { //当前节点为根节点，需要创建新根节点
			return split.splitToRootNode(rightNodePosition)
		} else { //右边节点的第一个key需要插入到父级中(需要做移动)
			return split.splitToKey(rightNodePosition, isBinary)
		}
	}
}

/*
	节点内部key变动
*/
func (split *TreeSplit) moveKey() error {
	keyPosition := split.Current
	updateKV := split.UpdateKV
	insertKV := split.InsertKV
	node := keyPosition.NodePosition.Node
	position := keyPosition.Position
	compare := keyPosition.Compare
	isWrite := false
	if updateKV != nil { //原值变更
		node.Keys[position] = updateKV.Key
		node.Values[position] = updateKV.Value
		isWrite = true
	}
	if insertKV != nil { //插入(除插入到最右边不需要移动，其他情况都需要移动)
		keyNum := int16(len(node.Keys))
		if compare == tree.CompareEq {//变更
			node.Keys[position] = insertKV.Key
			node.Values[position] = insertKV.Value
		}else if position+1 == keyNum { //插入到最右边
			node.Keys = append(node.Keys, insertKV.Key)
			node.Values = append(node.Values, insertKV.Value)
		} else { //插入中间，需要移动右边元素
			move := position//默认移动右边元素，即插入在右边，当前元素无需移动
			if compare == tree.CompareLt { //插入到左边，当前元素需要右移动，填充到当前位置
				if move > 0 {//防止下标为负数,最左边为0
					move--
				}
			} else if compare == tree.CompareGt { //插入到右边，填充到右边位置
				if move < 0 {//防止下标为负数,实际需要插入0的位置
					move = 0
				}
				position++
			}
			//动态增加数组长度
			node.Keys = append(node.Keys,nil)
			node.Values = append(node.Values,nil)
			//从move位置之后所有元素往右移动
			for i := keyNum; i > move; i-- {
				node.Keys[i] = node.Keys[i-1]
				node.Values[i] = node.Values[i-1]
			}
			node.Keys[position] = insertKV.Key
			node.Values[position] = insertKV.Value
		}
		split.Cache.keyNumIncrement()
		isWrite = true
	}
	if isWrite {
		//缓存待写入节点
		split.Cache.setWrite(keyPosition.NodePosition)
	}else{
		return fmt.Errorf("moveKey write rule not found")
	}
	return nil
}