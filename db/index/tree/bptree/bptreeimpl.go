package bptree

import (
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
// 3、叶子节点之间建立双向链表
// 解决方案：叶子节点增加指向左右兄弟的指针，在分裂之后需要更改左右兄弟的指向新节点的指针---待实现
// 4、目前leveldb是否实现双向链表，是否支持排序
// 解决方案：leveldb的memTable是双向链表，默认按照升序，如果降序需要反向扫描，但对应磁盘顺序读写的特性，反向性能会差
// Fabric合约接口目前没有提供反向查询
// 5、是否需要建立主键索引，此处修改比较大，目前大多数行数据在1-6K，可以单独存入一个key中
// 6、表修改影响行和索引，如何设计表修改策略
// 解决方案：表修改原有行和索引数据不做变更，新写入的行和索引需要按新的结构来，读取需要根据表最新结构来
// 行读取：读取到的行需要按表结构格式化
// 表更新：列类型转换需要查看当前列是否有值，有值无法转换，名字修改需要建立原列与新列关系以便老数据可以匹配到
// 其实行数据可以使用列索引加值方式，这样可以解决名字和类型频繁变动不影响原数据

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

func (service *BPTreeImpl) getHead(table string, column string, cache *TreeNodeCache) (*TreeHead, error) {
	if cache.Head != nil {
		return cache.Head, nil
	}
	headBytes, err := service.storage.GetHead(table, column)
	if err != nil {
		return nil, err
	}
	var head *TreeHead
	if headBytes == nil || len(headBytes) == 0 {
		head = &TreeHead{table, column, TREE_DEFAULT_TYPE, 0, 0, 0, 0, 0}
	} else {
		head = &TreeHead{}
		if err := json.Unmarshal(headBytes, head); err != nil {
			return nil, err
		}
	}
	cache.Head = head
	return head, nil
}

func (service *BPTreeImpl) getNodePosition(pointer Pointer, parent *TreeNodePosition, position Position, cache *TreeNodeCache) (*TreeNodePosition, error) {
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
		node := &TreeNode{}
		if err := json.Unmarshal(nodeBytes, node); err != nil {
			return nil, err
		}
		nodePosition = &TreeNodePosition{pointer, node, parent, position}
		if cache.IsWrite {
			cache.Read[pointer] = nodePosition
		}
	}
	return nodePosition, nil
}

func (service *BPTreeImpl) putNode(cache *TreeNodeCache) error {
	table := cache.Head.Table
	column := cache.Head.Column
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
		keyData, err := BinarySearch(rootNodePosition, key)
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
	keyData, err := BinarySearch(nodePosition, key)
	if err != nil {
		return nil, err
	}
	if nodePosition.Node.Type == NodeTypeLeaf { //递归到最底层叶子节点
		return &keyData, nil
	} else { //递归查找下级节点
		lower, err := service.getNodePosition(ToPointer(keyData.Value), nodePosition, keyData.KeyPosition.Position, cache)
		if err != nil {
			return nil, err
		}
		return service.recursionNode(lower, key, cache)
	}
}

func (service *BPTreeImpl) printNode(nodePosition *TreeNodePosition, height int8, str []string, cache *TreeNodeCache) ([]string, error) {
	space := ""
	height = height + 2
	node := nodePosition.Node
	for i, k := range node.Keys {
		value := node.Values[i]
		printKey := ""
		pointer := Pointer(0)
		if node.Type != NodeTypeLeaf {
			pi, _, err := ParseValue(value)
			if err != nil {
				return nil, err
			}
			pointer = ToPointer(pi)
			printKey = fmt.Sprintf("(%v->&%d) %s", k, pointer, space)
		} else {
			printKey = fmt.Sprintf("(%v:%v) %s", k, value, space)
		}
		if i == 0 {
			printKey = fmt.Sprintf("&%d【 "+printKey, nodePosition.Pointer)
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
			str, err = service.printNode(childNode, height, str, cache)
			if err != nil {
				return nil, err
			}
		}
	}
	return str, nil
}

//////////////////// Implement Tree Interface ///////////////////////////////
func (service *BPTreeImpl) Search(table string, column string, key []byte) (interface{}, tree.ValueType, error) {
	cache := createTreeNodeCache(nil, false)
	head, err := service.getHead(table, column, cache)
	if err != nil {
		return nil, tree.ValueTypeNone, err
	}
	if TreeIsNull(head) {
		return nil, tree.ValueTypeNone, fmt.Errorf("tree is null")
	}
	keyData, err := service.findPosition(key, cache)
	if err != nil {
		return nil, tree.ValueTypeNone, err
	}
	if keyData.KeyPosition.Compare == CompareEq {
		return keyData.Value, keyData.ValueType, nil
	}
	return nil, tree.ValueTypeNone, nil
}

/*
	插入key到树中
*/
func (service *BPTreeImpl) Insert(table string, column string, key []byte, value interface{}, insertType tree.InsertType) error {
	cache := createTreeNodeCache(nil, true)
	head, err := service.getHead(table, column, cache)
	if err != nil {
		return err
	}
	if TreeIsNull(head) {
		rootPosition, err := createNodePosition(NodeTypeLeaf, nil, 0, [][]byte{}, [][]byte{}, cache)
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
	convertValue, err := ConvertValue(value, insertType, keyData)
	if err != nil {
		return err
	}
	//叶子节点写入key并平衡树结构
	err = balanceTree(&TreeSplitData{nil,&keyData.KeyPosition,nil,&TreeNodeKV{key, convertValue}}, cache)
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

func (service *BPTreeImpl) Print(table string, column string) error {
	cache := createTreeNodeCache(nil, false)
	head, err := service.getHead(table, column, cache)
	if err != nil {
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
	str, err = service.printNode(nodePosition, -1, str, cache)
	if err != nil {
		return err
	}
	fmt.Println(util.ConvertJsonString(*head))
	for _, v := range str {
		fmt.Println(v)
	}
	return nil
}
