package bptree

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/database-fabric/db"
	"github.com/database-fabric/db/index/tree"
	"github.com/database-fabric/db/storage"
	"github.com/database-fabric/db/util"
)

/////////////////// Service Function ///////////////////
type BPTreeImpl struct {
	storage *storage.BPTreeStorage
	iValue tree.ValueInterface
}

func NewBPTreeImpl(storage *storage.BPTreeStorage, iValue tree.ValueInterface) *BPTreeImpl {
	return &BPTreeImpl{storage,iValue}
}

///////////////////////// Storage Function //////////////////////

func (service *BPTreeImpl) getHead(key db.ColumnKey) (*tree.TreeHead, error) {
	headBytes, err := service.storage.GetHead(key)
	if err != nil {
		return nil, err
	}
	var head *tree.TreeHead
	if headBytes != nil && len(headBytes) > 0{
		head = &tree.TreeHead{}
		if err := json.Unmarshal(headBytes, head); err != nil {
			return nil, err
		}
	}
	return head, nil
}

func (service *BPTreeImpl) createHead(key db.ColumnKey, treeType tree.TreeType, cache *TreeNodeCache) (*tree.TreeHead, error) {
	if cache != nil && cache.Head != nil {
		return cache.Head, nil
	}
	head, err := service.getHead(key)
	if err != nil {
		return nil, err
	}
	if head == nil  {
		head = &tree.TreeHead{Key:key, Type:treeType}
	}
	if cache != nil {
		cache.Head = head
	}
	return head, nil
}

func (service *BPTreeImpl) getNode(pointer tree.Pointer, head *tree.TreeHead) (*tree.TreeNode, error) {
	nodeBytes, err := service.storage.GetNode(head.Key, util.Int64ToString(int64(pointer)))
	if err != nil {
		return nil, err
	}
	if nodeBytes == nil || len(nodeBytes) == 0 {
		return nil, fmt.Errorf("node `%d` not found", pointer)
	} else {
		node := &tree.TreeNode{}
		if err := json.Unmarshal(nodeBytes, node); err != nil {
			return nil, err
		}
		return node,nil
	}
}

func (service *BPTreeImpl) getNodePosition(pointer tree.Pointer, parent *TreeNodePosition, position Position, cache *TreeNodeCache) (*TreeNodePosition, error) {
	nodePosition, ok := cache.Read[pointer]
	if ok {
		return nodePosition, nil
	}
	node, err := service.getNode(pointer, cache.Head); if err != nil {
		return nil, err
	}
	nodePosition = &TreeNodePosition{pointer, node, parent,nil,nil,position}
	if cache.IsWrite {
		cache.Read[pointer] = nodePosition
	}
	return nodePosition, nil
}

func (service *BPTreeImpl) putNode(cache *TreeNodeCache) error {
	//fmt.Println(fmt.Sprintf("write node length %d", len(cache.Write)))
	for pointer, nodePosition := range cache.Write {
		//fmt.Println(nodePosition.Node.Keys)
		//fmt.Println(nodePosition.Node.Values)
		//fmt.Println(util.ConvertJsonString(*nodePosition.Node))
		nodeBytes, err := util.ConvertJsonBytes(*nodePosition.Node)
		if err != nil {
			return err
		}
		if err := service.storage.PutNode(cache.Head.Key, util.Int64ToString(int64(pointer)), nodeBytes); err != nil {
			return err
		}
	}
	headBytes, err := util.ConvertJsonBytes(*cache.Head)
	if err != nil {
		return err
	}
	if err := service.storage.PutHead(cache.Head.Key, headBytes); err != nil {
		return err
	}
	return nil
}

func (service *BPTreeImpl) linkPosition(nodePosition *TreeNodePosition, cache *TreeNodeCache)  error {
	if nodePosition.Node.Prev > tree.Pointer(0) {
		prev, err := service.getNodePosition(nodePosition.Node.Prev, nodePosition.Parent, nodePosition.Position-1, cache)
		if err != nil {
			return err
		}
		nodePosition.Prev = prev
	}
	if nodePosition.Node.Next > tree.Pointer(0) {
		next, err := service.getNodePosition(nodePosition.Node.Next, nodePosition.Parent, nodePosition.Position+1, cache)
		if err != nil {
			return err
		}
		nodePosition.Next = next
	}
	return nil
}

/*
	查询key所在叶子节点位置
	从根开始递归查找(用二分法比较key大小)，直到找到叶子节点
	返回最近节点、最近节点key、最近key位置、比较结果，比较结果返回三种情况(等于：找到key位置，大于：右边，小于：左边)
*/
func (service *BPTreeImpl) findPosition(key []byte, cache *TreeNodeCache) (*TreeKeyData, error) {
	rootNodePosition, err := service.getNodePosition(cache.Head.Root, nil, -1, cache)
	if err != nil {
		return nil, err
	}
	if cache.Head.Height == 1 { //树阶数为1，叶子节点为根节点
		keyData, err := rootNodePosition.binarySearch(key)
		if err != nil {
			return nil, err
		}
		return &keyData, nil
	} else { //从根遍历查找到最底层叶子节点位置
		return service.recursionNode(rootNodePosition, key, cache)
	}
}

func (service *BPTreeImpl) recursionNode(nodePosition *TreeNodePosition, key []byte, cache *TreeNodeCache) (*TreeKeyData, error) {
	var keyData TreeKeyData
	var err error
	for i:=int8(0);i<cache.Head.Height;i++ {
		temp := nodePosition
		if len(temp.Node.Keys) == 0 { //节点为空，无法递归
			return nil, fmt.Errorf("recursionNode node `%d` is null", temp.Pointer)
		}
		keyData,err = temp.binarySearch(key)
		if err != nil {
			return nil, err
		}
		if temp.Node.Type == tree.NodeTypeLeaf { //递归到最底层叶子节点
			return &keyData, nil
		} else { //递归查找下级节点
			nodePosition,err = service.getNodePosition(tree.BytesToPointer(keyData.Data.Value), temp, keyData.KeyPosition.Position, cache)
			if err != nil {
				return nil, err
			}
		}
	}
	return &keyData,nil
}

/*
	节点内部key区间查找，排序为升序
	返回匹配的列表，是否全部匹配到(标记是否循环)
*/
func (service *BPTreeImpl) rangeSearchByAsc(node *tree.TreeNode, position Position, endKey []byte, size *tree.Pointer, list *[]*db.KV) (bool,error) {
	if *size == 0 {//数量已经满足
		return false,nil
	}
	isLoop := true
	for i:=position;i<Position(len(node.Keys));i++{
		key := node.Keys[i]
		compare := -1 //默认为小于
		if len(endKey) > 0 {//endKey值不为空比较区间是否超出
			compare = bytes.Compare(key, endKey)
		}
		if compare != 1 { //小于或等于
			kv,err := service.parseValue(key, node.Values[i]); if err != nil {
				return false,err
			}
			*list = append(*list, kv)
			*size--
			if *size == 0 {//数量已经满足
				return false,nil
			}
		}
		if compare == 0 || compare == 1 { //等于或大于
			isLoop = false
			break
		}
	}
	return isLoop,nil
}

/*
	节点内部key区间查找，排序为降序
	返回匹配的列表，是否全部匹配到(标记是否循环)
*/
func (service *BPTreeImpl) rangeSearchByDesc(node *tree.TreeNode, position Position, endKey []byte, size *tree.Pointer, list *[]*db.KV) (bool,error) {
	if *size == 0 {//数量已经满足
		return false,nil
	}
	isLoop := true
	for i:=position;i>=0;i--{
		key := node.Keys[i]
		compare := 1 //默认为大于
		if len(endKey) > 0 {//endKey值不为空比较区间是否超出
			compare = bytes.Compare(key, endKey)
		}
		if compare != -1 { //大于或等于
			kv,err := service.parseValue(key, node.Values[i]); if err != nil {
				return false,err
			}
			*list = append(*list, kv)
			*size--
			if *size == 0 {//数量已经满足
				return false,nil
			}
		}
		if compare == 0 || compare == -1 { //等于或小于
			isLoop = false
			break
		}
	}
	return isLoop,nil
}

func (service *BPTreeImpl) printNode(nodePosition *TreeNodePosition, height int8, str *[]string, printData bool, cache *TreeNodeCache) error {
	if height >= int8(len(*str)) {
		return nil
	}
	space := ""
	node := nodePosition.Node
	height = height + 1

	for i, k := range node.Keys {
		value := node.Values[i]
		printKey := ""
		pointer := tree.Pointer(0)
		if node.Type != tree.NodeTypeLeaf {
			pointer = tree.BytesToPointer(value)
			if printData {
				printKey = fmt.Sprintf("(%v->&%d) %s", k, pointer, space)
			}
		} else if printData {
			kv, err := service.parseValue(k, value);
			if err != nil {
				return err
			}
			cv, err := service.iValue.ToString(kv.Value, kv.VType);
			if err != nil {
				return err
			}
			printKey = fmt.Sprintf("(%v:%s) %s", k, cv, space)
		}
		if i == 0 {
			size,err := GetNodeSize(node); if err != nil {
				return err
			}
			printKey = fmt.Sprintf("&%d(size:%d,len:%d,prev:%d,next:%d)【 %s", nodePosition.Pointer, size, len(node.Keys), node.Prev, node.Next, printKey)
		}
		if i == len(node.Keys)-1 {
			printKey = printKey + "】     "
		}
		(*str)[height-1] = (*str)[height-1] + printKey
		if pointer > 0 {
			next, err := service.getNodePosition(pointer, nodePosition, -1, cache)
			if err != nil {
				return  err
			}
			err = service.printNode(next, height, str, printData, cache); if err != nil {
				return  err
			}
		}
	}
	return nil
}

func (service *BPTreeImpl) parseValue(key []byte, value []byte) (*db.KV,error) {
	kv := &db.KV{Key:key,Value:value}
	if err := service.iValue.Parse(kv); err != nil {
		return nil,err
	}
	return kv,nil
}

//////////////////// Implement Tree Interface ///////////////////////////////
/**
	创建树头
 */
func (service *BPTreeImpl) CreateHead(key db.ColumnKey, treeType tree.TreeType) (*tree.TreeHead, error) {
	return service.createHead(key, treeType,nil)
}

/**
	查询树头
*/
func (service *BPTreeImpl) SearchHead(key db.ColumnKey) (*tree.TreeHead, error)  {
	return service.getHead(key)
}

/*
	插入key到树中
*/
func (service *BPTreeImpl) Insert(head *tree.TreeHead, key []byte, value []byte, insertType tree.InsertType) (*tree.RefNode,error) {
	cache,err := createTreeNodeCache(head,true); if err != nil {
		return nil,err
	}
	if TreeIsNull(head) {
		rootPosition, err := createNodePosition(tree.NodeTypeLeaf, nil, 0, [][]byte{}, [][]byte{}, cache)
		if err != nil {
			return nil,err
		}
		head.Root = rootPosition.Pointer
		head.Height = 1
	}
	keyData, err := service.findPosition(key, cache)
	if err != nil {
		return nil,err
	}
	//处理叶子节点多种数据类型
	var oldKV *db.KV
	if keyData.KeyPosition.Compare == tree.CompareEq {
		oldKV,err = service.parseValue(keyData.Data.Key, keyData.Data.Value); if err != nil {
			return nil,err
		}
	}
	kv := &db.KV{Key:key,Value:value}
	refNode,err := service.iValue.Format(kv, oldKV, insertType); if err != nil {
		return nil,err
	}
	//引用节点是否触发更新关键字值
	if refNode != nil && !refNode.Update {
		return refNode,nil
	}
	//叶子节点写入key并平衡树结构
	split := &TreeSplit{nil,&keyData.KeyPosition,nil,&TreeNodeKV{kv.Key,kv.Value},cache}
	isSplit,err := split.GetIsSplit(); if err != nil {
		return nil,err
	}
	if isSplit {//如果需要分裂需要缓存兄弟节点数据
		service.linkPosition(keyData.KeyPosition.NodePosition, cache)
	}
	err = split.balanceTree(isSplit)
	if err != nil {
		return nil,err
	}

	//保存所有需要变更的节点
	err = service.putNode(cache)
	if err != nil {
		return nil,err
	}
	return refNode,nil
}

/**
	查询key
 */
func (service *BPTreeImpl) Search(head *tree.TreeHead, key []byte) (*db.KV, error) {
	cache,err := createTreeNodeCache(head,false); if err != nil {
		return nil,err
	}
	if TreeIsNull(head) {
		return nil, fmt.Errorf("tree is null")
	}
	keyData, err := service.findPosition(key, cache)
	if err != nil {
		return nil, err
	}

	if keyData.KeyPosition.Compare == tree.CompareEq {
		kv,err := service.parseValue(keyData.Data.Key, keyData.Data.Value); if err != nil {
			return nil,err
		}
		return kv, nil
	}
	return nil, nil
}

/**
	区间查询，支持排序，分页
	升序：startKey为空默认为最左，endKey为空默认为最右
	降序：startKey为空默认为最右，endKey为空默认为最左
*/
func (service *BPTreeImpl) SearchByRange(head *tree.TreeHead, startKey []byte, endKey []byte, order db.OrderType, size tree.Pointer) ([]*db.KV, error) {
	var err error
	var node *tree.TreeNode
	position := Position(0)
	if order == db.ASC {
		if len(startKey) == 0 {
			node,err = service.getNode(head.FirstLeaf, head); if err != nil {
				return nil,err
			}
			startKey = node.Keys[position]
		}
		if len(endKey) > 0 && bytes.Compare(endKey, startKey) != 1 {
			return nil,fmt.Errorf("asc must startKey `%v` <= endKey `%v`", startKey, endKey)
		}
	}else{
		if len(startKey) == 0 {
			node,err = service.getNode(head.LastLeaf, head); if err != nil {
				return nil,err
			}
			position = Position(len(node.Keys)-1)
			startKey = node.Keys[position]
		}
		if  len(endKey) > 0 && bytes.Compare(startKey, endKey) != 1 {
			return nil,fmt.Errorf("desc must endKey `%v` <= startKey `%v`", endKey, startKey)
		}
	}
	if node == nil {
		cache,err := createTreeNodeCache(head,false); if err != nil {
			return nil,err
		}
		if TreeIsNull(head) {
			return nil, fmt.Errorf("tree is null")
		}
		keyData, err := service.findPosition(startKey, cache)
		if err != nil {
			return nil, err
		}
		node = keyData.KeyPosition.NodePosition.Node
		position = keyData.KeyPosition.Position
		compare := keyData.KeyPosition.Compare
		if compare == tree.CompareLt && order == db.DESC {//小于，降序需要往左移
			position--
		}else if compare == tree.CompareGt && order == db.ASC {//大于，升序需要往右移
			position++
		}
	}
	if node != nil {
		list := make([]*db.KV, 0, size)
		isLoop := true
		for isLoop {
			pointer := tree.Pointer(0)
			if order == db.ASC {
				pointer = node.Next
				if position < Position(len(node.Keys)) {
					isLoop,err = service.rangeSearchByAsc(node, position, endKey, &size, &list); if err != nil {
						return nil,err
					}
				}
			}else{
				pointer = node.Prev
				if position >= 0 {
					isLoop,err = service.rangeSearchByDesc(node, position, endKey, &size, &list); if err != nil {
						return nil,err
					}
				}
			}
			node = nil//查询完node设置为空释放内存
			if isLoop && pointer > tree.Pointer(0) {
				node,err = service.getNode(pointer, head); if err != nil {
					return nil,err
				}
				if order == db.ASC {
					position = Position(0)
				}else{
					position = Position(len(node.Keys)-1)
				}
			}else{
				isLoop = false
			}
		}
		return list, nil
	}
	return nil, nil
}

func (service *BPTreeImpl) Print(head *tree.TreeHead, printData bool) error {
	cache,err := createTreeNodeCache(head,false); if err != nil {
		return err
	}
	if TreeIsNull(head) {
		return fmt.Errorf("tree is null")
	}
	nodePosition, err := service.getNodePosition(head.Root, nil, -1, cache)
	if err != nil {
		return err
	}

	str := make([]string, head.Height*2)
	err = service.printNode(nodePosition,0, &str, printData, cache)
	if err != nil {
		return err
	}
	fmt.Println(util.ConvertJsonString(*head))
	fmt.Println()
	for _, v := range str {
		fmt.Println(v)
		fmt.Println()
	}
	return nil
}
