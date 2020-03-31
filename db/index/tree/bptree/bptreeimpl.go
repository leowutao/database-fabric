package bptree

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db/index/tree"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

// TODO 存在的问题
// 1、外建索引中主键值过多，如叶子节点数据过大是否需要二级树来支撑
// 解决方案：定义节点key值大小，根据节点容量/4，如果超过建立数据节点来存储--待实现
// 2、唯一索引如何建立
// 解决方案：key值不能过大，正常在20个字节左右最佳，如果过大节点会成倍增长，验证只要匹配到位置即返回，插入还是需要到叶子节点
// 3、叶子节点之间建立双向链表-已解决
// 解决方案：叶子节点增加指向左右兄弟的指针，在分裂之后需要更改左右兄弟的指向新节点的指针---待实现
// 4、目前leveldb是否实现双向链表，是否支持排序
// 解决方案：leveldb的memTable是双向链表，默认按照升序，如果降序需要反向扫描，但对应磁盘顺序读写的特性，反向性能会差
// Fabric合约接口目前没有提供反向查询
// 目前使用叶子节点双向链表解决，虽然反向查询可能慢，后期可以提供降序索引
// 5、是否需要建立主键索引，此处修改比较大，目前大多数行数据在1-6K，可以单独存入一个key中
// 解决方案：可以统一建立block来管理不同的数据，上限4kb，可以配置容量大小，
// 6、表修改影响行和索引，如何设计表修改策略
// 解决方案：表修改原有行和索引数据不做变更，新写入的行和索引需要按新的结构来，读取需要根据表最新结构来
// 行读取：读取到的行需要按表结构格式化
// 表更新：列类型转换需要查看当前列是否有值，有值无法转换，名字修改需要建立原列与新列关系以便老数据可以匹配到
// 其实行数据可以使用列索引加值方式，这样可以解决名字和类型频繁变动不影响原数据

// 重点：1、对所有的key类型归类，建立标识位来区分；2、所有动态key需要建立索引表来归类，由于组合key每次写入数据可能带入动态key会导致key过长，
// 尽量减少key所占block空间；3、行记录block可以建立区间标记位来区分，主键索引可以直接指向此block的指针；
// 4、历史记录需要按行纬度建立block

// TODO 可以忽略的问题
// 1、删除key，目前删除为逻辑删除，使用事务ID建立版本号，索引也不需要删除功能
// 2、私有数据不存在版本号，如果不是私有数据是否能使用自带的版本号
// 3、索引数据修改，如果按区块事务本身是排序的。不需要加锁处理，但同一个事务中批量处理如何一次性更改索引数据


/////////////////// Service Function ///////////////////
type BPTreeImpl struct {
	storage *storage.BPTreeStorage
}

func NewBPTreeImpl(storage *storage.BPTreeStorage) *BPTreeImpl {
	return &BPTreeImpl{storage}
}

///////////////////////// Storage Function //////////////////////

func (service *BPTreeImpl) getHead(table string, column string) (*tree.TreeHead, error) {
	headBytes, err := service.storage.GetHead(table, column)
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

func (service *BPTreeImpl) createHead(table string, column string, treeType tree.TreeType, cache *TreeNodeCache) (*tree.TreeHead, error) {
	if cache != nil && cache.Head != nil {
		return cache.Head, nil
	}
	head, err := service.getHead(table, column)
	if err != nil {
		return nil, err
	}
	if head == nil  {
		head = &tree.TreeHead{table,column,treeType,0,0,0,0,0}
	}
	if cache != nil {
		cache.Head = head
	}
	return head, nil
}

func (service *BPTreeImpl) getNodePosition(pointer tree.Pointer, parent *TreeNodePosition, position Position, cache *TreeNodeCache) (*TreeNodePosition, error) {
	nodePosition, ok := cache.Read[pointer]
	if ok {
		return nodePosition, nil
	}
	nodeBytes, err := service.storage.GetNode(cache.Head.Table, cache.Head.Column, util.Int64ToString(int64(pointer)))
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
		nodePosition = &TreeNodePosition{pointer, node, parent,nil,nil,position}
		if cache.IsWrite {
			cache.Read[pointer] = nodePosition
		}
	}
	return nodePosition, nil
}

func (service *BPTreeImpl) putNode(cache *TreeNodeCache) error {
	table := cache.Head.Table
	column := cache.Head.Column
	//fmt.Println(fmt.Sprintf("write node length %d", len(cache.Write)))
	for pointer, nodePosition := range cache.Write {
		//fmt.Println(nodePosition.Node.Keys)
		//fmt.Println(nodePosition.Node.Values)
		//fmt.Println(t.ConvertJsonString(*nodePosition.Node))
		nodeBytes, err := util.ConvertJsonBytes(*nodePosition.Node)
		if err != nil {
			return err
		}
		if err := service.storage.PutNode(table, column, util.Int64ToString(int64(pointer)), nodeBytes); err != nil {
			return err
		}
	}
	headBytes, err := util.ConvertJsonBytes(*cache.Head)
	if err != nil {
		return err
	}
	if err := service.storage.PutHead(table, column, headBytes); err != nil {
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
	if len(nodePosition.Node.Keys) == 0 { //节点为空，无法递归
		return nil, fmt.Errorf("recursionNode node `%d` is null", nodePosition.Pointer)
	}
	keyData, err := nodePosition.binarySearch(key)
	if err != nil {
		return nil, err
	}
	if nodePosition.Node.Type == tree.NodeTypeLeaf { //递归到最底层叶子节点
		return &keyData, nil
	} else { //递归查找下级节点
		lower, err := service.getNodePosition(tree.BytesToPointer(keyData.Data.Value), nodePosition, keyData.KeyPosition.Position, cache)
		if err != nil {
			return nil, err
		}
		return service.recursionNode(lower, key, cache)
	}
}

func (service *BPTreeImpl) printNode(nodePosition *TreeNodePosition, height int8, str []string, printData bool, cache *TreeNodeCache) ([]string, error) {
	space := ""
	height = height + 2
	node := nodePosition.Node
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
			v,vtype := ParseValue(value)
			cv,err := tree.ParseValueToString(v, vtype); if err != nil {
				return nil,err
			}
			printKey = fmt.Sprintf("(%v:%s) %s", k, cv, space)
		}
		if i == 0 {
			printKey = fmt.Sprintf("&%d(prev:%d,next:%d)【 %s", nodePosition.Pointer, node.Prev, node.Next, printKey)
		}
		if i == len(node.Keys)-1 {
			printKey = printKey + "】     "
		}
		str[height-1] = str[height-1] + printKey
		//printValue := fmt.Sprintf("%v%s", value, space)
		//str[height] = str[height] + printValue
		if pointer > 0 {
			childNode, err := service.getNodePosition(pointer, nodePosition, -1, cache)
			if err != nil {
				return nil, err
			}
			str, err = service.printNode(childNode, height, str, printData, cache)
			if err != nil {
				return nil, err
			}
		}
	}
	return str, nil
}

//////////////////// Implement Tree Interface ///////////////////////////////
/**
	创建树头
 */
func (service *BPTreeImpl) CreateHead(table string, column string, treeType tree.TreeType) (*tree.TreeHead, error) {
	return service.createHead(table, column, treeType,nil)
}

/**
	查询树头
*/
func (service *BPTreeImpl) SearchHead(table string, column string) (*tree.TreeHead, error)  {
	return service.getHead(table, column)
}

/*
	插入key到树中
*/
func (service *BPTreeImpl) Insert(head *tree.TreeHead, key []byte, value []byte, insertType tree.InsertType) error {
	cache,err := createTreeNodeCache(head,true); if err != nil {
		return err
	}
	if TreeIsNull(head) {
		rootPosition, err := createNodePosition(tree.NodeTypeLeaf, nil, 0, [][]byte{}, [][]byte{}, cache)
		if err != nil {
			return err
		}
		head.Root = rootPosition.Pointer
		head.Height = 1
	}
	keyData, err := service.findPosition(key, cache)
	if err != nil {
		return err
	}
	//处理叶子节点多种数据类型
	var oldValue []byte
	if keyData.KeyPosition.Compare == tree.CompareEq {
		oldValue = keyData.Data.Value
	}
	convertValue, err := tree.ConvertValue(key, value, oldValue, keyData.ValueType, insertType)
	if err != nil {
		return err
	}
	//叶子节点写入key并平衡树结构
	split := &TreeSplit{nil,&keyData.KeyPosition,nil,&TreeNodeKV{key,convertValue},cache}
	isSplit,err := split.GetIsSplit(); if err != nil {
		return err
	}
	if isSplit {//如果需要分裂需要缓存兄弟节点数据
		service.linkPosition(keyData.KeyPosition.NodePosition, cache)
	}
	err = split.balanceTree(isSplit)
	if err != nil {
		return err
	}

	//保存所有需要变更的节点
	err = service.putNode(cache)
	if err != nil {
		return err
	}
	return nil
}

/**
	查询key
 */
func (service *BPTreeImpl) Search(head *tree.TreeHead, key []byte) ([]byte, error) {
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
		return keyData.Data.Value, nil
	}
	return nil, nil
}

/**
查询key
*/
func (service *BPTreeImpl) SearchByRange(head *tree.TreeHead, startKey []byte, endKey []byte, size tree.Pointer) ([]tree.KV, error) {
	if bytes.Compare(endKey, startKey) != 1 {
		return nil,fmt.Errorf("startKey `%v` > endKey `%v`", startKey, endKey)
	}
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

	if keyData.KeyPosition.Compare == tree.CompareEq {
		list := &[]tree.KV{}
		rangePosition := keyData.KeyPosition.Position
		nodePosition := keyData.KeyPosition.NodePosition
		isNext := true
		for isNext{
			var err error
			node := nodePosition.Node
			isNext,err = nodePosition.rangeSearch(rangePosition, endKey, &size, list); if err != nil {
				return nil,err
			}
			if isNext && node.Next > tree.Pointer(0) {
				nodePosition, err = service.getNodePosition(node.Next, nodePosition.Parent, nodePosition.Position+1, cache)
				if err != nil {
					return nil,err
				}
				rangePosition = 0
			}else{
				isNext = false
			}
		}
		return *list, nil
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
	str, err = service.printNode(nodePosition, -1, str, printData, cache)
	if err != nil {
		return err
	}
	fmt.Println(util.ConvertJsonString(*head))
	for _, v := range str {
		fmt.Println(v)
	}
	return nil
}
