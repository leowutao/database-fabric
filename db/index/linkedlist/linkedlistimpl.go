package linkedlist

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

/////////////////// Service Function ///////////////////
type LinkedListImpl struct {
	storage *storage.LinkedListStorage
}

func NewLinkedListImpl(storage *storage.LinkedListStorage) *LinkedListImpl {
	return &LinkedListImpl{storage}
}

///////////////////////// Storage Function //////////////////////

func (service *LinkedListImpl) getHead(key db.ColumnRowKey) (*LinkedHead, error) {
	headBytes, err := service.storage.GetHead(key)
	if err != nil {
		return nil, err
	}
	var head *LinkedHead
	if headBytes != nil && len(headBytes) > 0{
		head = &LinkedHead{}
		if err := json.Unmarshal(headBytes, head); err != nil {
			return nil, err
		}
	}
	return head, nil
}

func (service *LinkedListImpl) createHead(key db.ColumnRowKey) (*LinkedHead, error) {
	head, err := service.getHead(key)
	if err != nil {
		return nil, err
	}
	if head == nil  {
		head = &LinkedHead{Key:key}
	}
	return head, nil
}

func (service *LinkedListImpl) getNode(pointer Pointer, head *LinkedHead) (*LinkedNode, error) {
	nodeBytes, err := service.storage.GetNode(head.Key, util.Int64ToString(int64(pointer)))
	if err != nil {
		return nil, err
	}
	if nodeBytes == nil || len(nodeBytes) == 0 {
		return nil, fmt.Errorf("node `%d` not found", pointer)
	} else {
		node := &LinkedNode{}
		if err := json.Unmarshal(nodeBytes, node); err != nil {
			return nil, err
		}
		return node,nil
	}
}

func (service *LinkedListImpl) createNode(prevNode *LinkedNode, head *LinkedHead) (Pointer,*LinkedNode) {
	pointer := head.Order + 1
	node := &LinkedNode{Prev:head.Order}
	if head.Order == 0 {
		head.First = pointer
	}
	head.Last = pointer
	head.Order = pointer
	if prevNode != nil {
		prevNode.Next = pointer
	}
	return pointer,node
}

func (service *LinkedListImpl) putNode(nodes map[Pointer]*LinkedNode, head *LinkedHead) error {
	for pointer,node := range nodes {
		nodeBytes, err := util.ConvertJsonBytes(*node)
		if err != nil {
			return err
		}
		if err := service.storage.PutNode(head.Key, util.Int64ToString(int64(pointer)), nodeBytes); err != nil {
			return err
		}
		node = nil
	}
	headBytes, err := util.ConvertJsonBytes(*head)
	if err != nil {
		return err
	}
	if err := service.storage.PutHead(head.Key, headBytes); err != nil {
		return err
	}
	head = nil
	return nil
}

func GetNodeSize(node *LinkedNode) (int, error) {
	nodeBytes, err := util.ConvertJsonBytes(*node)
	if err != nil {
		return 0, err
	}
	nodeSize := len(nodeBytes) + NODE_NAME_SIZE
	return nodeSize, nil
}

func IsSplit(value []byte, node *LinkedNode) (bool, error) {
	nodeSize, err := GetNodeSize(node)
	if err != nil {
		return false, err
	}
	addSize := len(value)
	if nodeSize+addSize > MAX_NODE_SIZE {
		return true,nil
	}
	return false,nil
}

func (service *LinkedListImpl) rangeSearchByOrder(node *LinkedNode, order db.OrderType, size *Pointer, list *[][]byte) (bool,error) {
	if *size == 0 {//数量已经满足
		return false,nil
	}
	isLoop := true
	length := len(node.Values)
	i := 0
	if order == db.DESC {
		i = length-1
	}
	for {
		value := node.Values[i]
		*list = append(*list, value)
		*size--
		if *size == 0 {//数量已经满足
			return false,nil
		}
		if order == db.DESC {
			i--
			if i < 0 {
				break
			}
		}else{
			i++
			if i >= length {
				break
			}
		}
	}
	return isLoop,nil
}

//////////////////// Implement LinkedList Interface ///////////////////////////////
/**
	创建链表头
 */
func (service *LinkedListImpl) CreateHead(key db.ColumnRowKey) (*LinkedHead, error) {
	return service.createHead(key)
}

/**
	查询链表头
*/
func (service *LinkedListImpl) SearchHead(key db.ColumnRowKey) (*LinkedHead, error)  {
	return service.getHead(key)
}

/*
	插入值到链表中,只允许顺序插入(排序需要外部控制)
*/
func (service *LinkedListImpl) Insert(head *LinkedHead, values [][]byte) error {
	if head == nil {
		return fmt.Errorf("linkedlist head is null")
	}
	var err error
	var pointer Pointer
	var node *LinkedNode
	head.Num = head.Num + int64(len(values))
	if head.Order == 0 {
		pointer,node = service.createNode(nil, head)
	}else{
		pointer = head.Last
		node,err = service.getNode(pointer, head); if err != nil {
			return err
		}
	}
	nodes := make(map[Pointer]*LinkedNode)
	nodes[pointer] = node
	temp := make([][]byte, 0, len(values))
	for i,value := range values {
		split,err := IsSplit(value, node); if err != nil {
			return err
		}
		if split {
			node.Values = append(node.Values, temp...)
			temp = make([][]byte, 0, len(values)-i)
			pointer,node = service.createNode(node, head)
			nodes[pointer] = node
		}
		temp = append(temp, value)
	}
	node.Values = append(node.Values, temp...)
	return service.putNode(nodes, head)
}

/**
	分页查询，支持正向和逆向
*/
func (service *LinkedListImpl) SearchByRange(head *LinkedHead, order db.OrderType, size Pointer) ([][]byte,db.Total,error){
	if head == nil {
		return nil,0,fmt.Errorf("linkedlist head is null")
	}
	var err error
	var node *LinkedNode
	if order == db.ASC {
		node,err = service.getNode(head.First, head); if err != nil {
			return nil,0,err
		}
	}else{
		node,err = service.getNode(head.Last, head); if err != nil {
			return nil,0,err
		}
	}
	if node != nil {
		list := make([][]byte, 0, size)
		for {
			pointer := Pointer(0)
			if order == db.ASC {
				pointer = node.Next
			}else{
				pointer = node.Prev
			}
			isLoop,err := service.rangeSearchByOrder(node, order, &size, &list); if err != nil {
				return nil,0,err
			}
			node = nil//查询完node设置为空释放内存
			if isLoop && pointer > Pointer(0) {
				node,err = service.getNode(pointer, head); if err != nil {
					return nil,0,err
				}
			}else{
				break
			}
		}
		return list,head.Num,nil
	}
	return nil,0,nil
}

func (service *LinkedListImpl) Print(head *LinkedHead) error {
	if head == nil {
		return fmt.Errorf("linkedlist head is null")
	}
	fmt.Println(util.ConvertJsonString(*head))
	pointer := head.First
	for pointer > 0 && pointer <= head.Last {
		node,err := service.getNode(pointer, head); if err != nil {
			return err
		}
		size,err := GetNodeSize(node); if err != nil {
			return err
		}
		printKey := fmt.Sprintf("&%d(size:%d,len:%d,prev:%d,next:%d)【 ", pointer, size, len(node.Values), node.Prev, node.Next)
		for _,v := range node.Values {
			printKey += fmt.Sprintf("%v,", v)
		}
		fmt.Println(printKey+"】")
		pointer = node.Next
	}
	return nil
}
