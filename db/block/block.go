package block

import (
	"encoding/json"
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"gitee.com/bidpoc/database-fabric-cc/db/util"
)

type BlockService struct {
	database *db.DataBase
	storage *storage.BlockStorage
	indexService *index.IndexService
}

func NewBlockService(database *db.DataBase, state state.ChainCodeState) *BlockService {
	indexService := index.NewIndexService(state)
	return &BlockService{database,storage.NewBlockStorage(state),indexService}
}

const(
	maxSize = 1024*4
	keySize = 25
	blockSize = 150
	useSize int64 = maxSize-keySize-blockSize
	rowSize = 25
)

//记录行数据切割位置(为了不对底层列值数据数组进行频繁copy，减少内存copy)
type BlockRowData struct {
	Row *db.RowData `json:"row"`//行数据指针
	//对行数据一纬数组切割索引，从1开始计算
	ColumnStart int16 `json:"columnStart"`//列开始位置
	ColumnEnd int16 `json:"columnEnd"`//列结束位置
	//对行数据二维数组切割索引，从1开始计算
	FirstDataStart int64 `json:"firstDataStart"`//第一个列值开始位置
	FirstDataEnd int64 `json:"firstDataEnd"`//第一个列值结束位置
	LastDataStart int64 `json:"lastDataStart"`//最后一个列值开始位置
	LastDataEnd int64 `json:"lastDataEnd"`//最后一个列值结束位置
}
//内存计算结构对应db中BlockData
type BlockData struct {
	Rows []BlockRowData `json:"rows"`
	Join db.JoinType `json:"join"`//与下一个块连接方式
}

func (service *BlockService) QueryRowBlockID(table *db.TableData, rowID db.RowID) (db.BlockID,error) {
	return service.indexService.GetPrimaryKeyIndex(service.database.Id, table, rowID)
}

func (service *BlockService) QueryRowIDByForeignKey(tableID db.TableID, foreignKey db.ForeignKey, referenceRowID db.RowID, size int32) ([]db.RowID,error) {
	return service.indexService.GetForeignKeyIndex(service.database.Id, tableID, foreignKey, referenceRowID, size)
}

func (service *BlockService) QueryRowDataByRange(table *db.TableData, start db.RowID, end db.RowID, order db.OrderType, size int32) ([]*db.RowData,error) {
	rowBlockIDList,err := service.indexService.GetPrimaryKeyIndexByRange(service.database.Id, table, start, end, order, size); if err != nil {
		return nil,err
	}
	rows := make([]*db.RowData, 0, len(rowBlockIDList))
	for _,rowBlockID := range rowBlockIDList {
		if rowBlockID.BlockID == 0 {
			rows = append(rows, &db.RowData{Id:rowBlockID.RowID})
		}else{
			row,err := service.getRowData(table.Id, rowBlockID.BlockID, rowBlockID.RowID); if err != nil {
				return nil,err
			}
			rows = append(rows, row)
		}
	}
	return rows,nil
}

func (service *BlockService) QueryRowDataHistoryByRange(table *db.TableData, rowID db.RowID, order db.OrderType, size int32) ([]*db.RowDataHistory,db.Total,error) {
	blocks,total,err := service.indexService.GetPrimaryKeyIndexHistoryByRange(service.database.Id, table, rowID, order, size); if err != nil {
		return nil,total,err
	}
	rows := make([]*db.RowDataHistory, 0, len(blocks))
	for _,blockID := range blocks {
		if blockID == 0 {
			rows = append(rows, &db.RowDataHistory{Row:&db.RowData{Id:rowID}})
		}else{
			row,err := service.getRowDataHistory(table.Id, blockID, rowID); if err != nil {
				return nil,total,err
			}
			rows = append(rows, row)
		}
	}
	return rows,total,nil
}

func (service *BlockService) QueryRowData(table *db.TableData, rowID db.RowID) (*db.RowData,error) {
	blockID,err := service.QueryRowBlockID(table, rowID); if err != nil {
		return nil,err
	}
	if blockID == 0 {
		return nil,nil
	}
	return service.getRowData(table.Id, blockID, rowID)
}


func (service *BlockService) getRowDataHistory(tableID db.TableID, blockID db.BlockID, rowID db.RowID) (*db.RowDataHistory,error) {
	block,err := service.getBlockData(tableID, blockID); if err != nil {
		return nil,err
	}
	row,err := service.joinBlockRowData(tableID, blockID, rowID, block); if err != nil {
		return nil,err
	}
	return &db.RowDataHistory{Tx:&block.TxData,Row:row},nil
}

func (service *BlockService) getRowData(tableID db.TableID, blockID db.BlockID, rowID db.RowID) (*db.RowData,error) {
	return service.joinBlockRowData(tableID, blockID, rowID,nil)
}

func (service *BlockService) getBlockData(tableID db.TableID, blockID db.BlockID) (*db.BlockData,error) {
	bytes,err := service.storage.GetBlockData(service.database.Id, tableID, blockID); if err != nil {
		return nil,err
	}
	if len(bytes) == 0 {
		return nil,fmt.Errorf("block `%d` is not found", blockID)
	}
	block := &db.BlockData{}
	if err := json.Unmarshal(bytes, block); err != nil {
		return nil,fmt.Errorf("block `%d` convert error `%s`", blockID, err.Error())
	}
	return block,nil
}

func (service *BlockService) joinRowData(row *db.RowData, joinRow *db.RowData, joinType db.JoinType) {
	index := len(row.Data)-1
	if joinType == db.JoinTypeRow {
		row.Data = append(row.Data, joinRow.Data...)
	}else if joinType == db.JoinTypeColumn {
		row.Data[index] = append(row.Data[index], joinRow.Data[0]...)
		if len(joinRow.Data) > 1 {
			row.Data = append(row.Data, joinRow.Data[1:]...)
		}
	}
}

func (service *BlockService) joinBlockRowData(tableID db.TableID, blockID db.BlockID, rowID db.RowID, block *db.BlockData) (*db.RowData,error) {
	if block == nil || block.Id == 0 {
		var err error
		block,err = service.getBlockData(tableID, blockID); if err != nil {
			return nil,err
		}
	}
	rowIndex := -1
	for i,row := range block.Rows {
		if row.Id == rowID {
			rowIndex = i
			break
		}
	}
	if rowIndex < 0 {
		return nil,fmt.Errorf("row `%d` is not found in block `%d`", blockID, rowID)
	}
	row := &block.Rows[rowIndex]
	if len(block.Rows) == (rowIndex+1) && block.Join != db.JoinTypeNone {//查找的行是块中最后一条并且块需要连接到下个块
		joinRow,err := service.joinBlockRowData(tableID, blockID+1, rowID,nil); if err != nil {
			return nil,err
		}
		service.joinRowData(row, joinRow, block.Join)
	}
	return row,nil
}

func (service *BlockService) combineRowData(use *int64, row *db.RowData, combineRows *[]BlockRowData, blocks *[]BlockData) {
	if *use <= 2*rowSize {
		if len(*combineRows) > 0 {
			*blocks = append(*blocks, BlockData{Rows:*combineRows})
			*combineRows = nil
		}
		*use = useSize
	}
	current := int16(1)
	end := int16(len(row.Data))
	temp := BlockRowData{Row:row,ColumnStart:current}
	for ;current<=end;current++ {
		*use = *use - rowSize
		size := int64(len(row.Data[current-1]))
		if *use == size {
			blockRow := temp
			blockRow.ColumnEnd = current
			*combineRows = append(*combineRows, blockRow)
			*blocks = append(*blocks, BlockData{Rows:*combineRows,Join:db.JoinTypeRow})
			*combineRows = nil
			temp = BlockRowData{Row:row,ColumnStart:current+1}
			*use = useSize
		}else if *use < size {
			blockRow := temp
			blockRow.ColumnEnd = current
			blockRow.LastDataStart = 1
			blockRow.LastDataEnd = *use
			*combineRows = append(*combineRows, blockRow)
			*blocks = append(*blocks, BlockData{Rows:*combineRows,Join:db.JoinTypeColumn})
			*combineRows = nil
			temp = BlockRowData{Row:row,ColumnStart:current}

			currentSize := size - *use
			cap := useSize+rowSize
			count := currentSize/cap
			have := currentSize%cap
			fmt.Println(currentSize,cap,count,have)
			for i:=int64(0);i<count;i++{
				blockRowLoop := temp
				blockRowLoop.ColumnEnd = current
				blockRowLoop.LastDataStart = *use+cap*i+1
				blockRowLoop.LastDataEnd = *use+cap*(i+1)
				join := db.JoinTypeColumn
				if i == count-1 && have == 0 {
					join = db.JoinTypeRow
				}
				*combineRows = append(*combineRows, blockRowLoop)
				*blocks = append(*blocks, BlockData{Rows:*combineRows,Join:join})
				*combineRows = nil
				temp = BlockRowData{Row:row,ColumnStart:current}
			}
			*use = useSize
			if have > 0 {
				temp.FirstDataStart = size - have + 1
				temp.FirstDataEnd = size
				*use -= have
			}else{
				temp.ColumnStart++
			}
		}else{
			*use = *use - size
		}
	}
	if (temp.ColumnStart == 1 && temp.FirstDataStart == 0 && temp.LastDataStart == 0) || (temp.FirstDataStart > 0 || temp.LastDataStart > 0) {
		temp.ColumnEnd = end
		*combineRows = append(*combineRows, temp)
	}
}

//func (service *BlockService) splitColumnData(use *int64, blockRow *BlockRowData, combineRows *[]*BlockRowData, blocks *[]BlockData) {
//	if *use <= 2*rowSize {
//		if len(*combineRows) > 0 {
//			*blocks = append(*blocks, BlockData{Rows:*combineRows})
//			*combineRows = nil
//		}
//		*use = useSize
//	}
//	*use = *use - rowSize
//	isSplit := false
//	length := blockRow.ColumnEnd
//	i := blockRow.ColumnStart
//	end := blockRow.ColumnEnd
//	splitData := blockRow.DataEnd > 0
//	for ;i<=end;i++ {
//		dataLength := int64(len(blockRow.Row.Data[i-1]))
//
//		size := dataLength
//		if splitData {
//			size = blockRow.DataEnd - blockRow.DataStart
//		}
//		if *use <= size {
//			join := db.JoinTypeRow
//			blockRow.ColumnEnd = i
//			newBlockRow := &BlockRowData{Row:blockRow.Row,ColumnStart:blockRow.ColumnEnd,ColumnEnd:length}
//			if *use < size {
//				join = db.JoinTypeColumn
//			}else{
//				newBlockRow.ColumnStart++
//				if newBlockRow.ColumnStart > length {//行最后一列已经计算完成，此块无需连接到下一个块
//					join = db.JoinTypeNone
//				}
//			}
//			if splitData || join == db.JoinTypeColumn {
//				blockRow.DataEnd = blockRow.DataStart + *use
//				if  join == db.JoinTypeColumn {
//					newBlockRow.DataStart = blockRow.DataEnd+1
//					newBlockRow.DataEnd = dataLength
//				}
//			}
//			*combineRows = append(*combineRows, blockRow)
//			*blocks = append(*blocks, BlockData{Rows:*combineRows,Join:join})
//			*combineRows = nil
//			*use = useSize
//			fmt.Println(blockRow.ColumnStart,blockRow.ColumnEnd,blockRow.DataStart,blockRow.DataEnd,join)
//			fmt.Println(newBlockRow.ColumnStart,newBlockRow.ColumnEnd,newBlockRow.DataStart,newBlockRow.DataEnd)
//			if newBlockRow.ColumnStart <= length {
//				service.splitRowData(use, newBlockRow, combineRows, blocks)
//			}
//			isSplit = true
//			break
//		}else{
//			*use = *use - size
//		}
//	}
//	if !isSplit {//最后一次未分裂数据追加
//		*combineRows = append(*combineRows, blockRow)
//	}
//}

func (service *BlockService) SetBlockData(table *db.TableData, tally *db.TableTally, rows []*db.RowData) error {
	txID,timestamp,err := service.storage.GetTxID(); if err != nil {
		return err
	}
	use := useSize
	var combineRows []BlockRowData
	var blocks []BlockData
	for i:=0;i<len(rows);i++ {
		service.rowTally(tally, rows[i])
		service.combineRowData(&use, rows[i], &combineRows, &blocks)
	}
	if len(combineRows) > 0 {
		blocks = append(blocks, BlockData{Rows:combineRows})
	}
	id := tally.Block
	rowIDMap := make(map[db.RowID]db.BlockID, len(rows))
	for _,b := range blocks {
		id++
		rows := make([]db.RowData, 0, len(b.Rows))
		for _,blockRow := range b.Rows {
			fmt.Println(blockRow.ColumnStart,blockRow.ColumnEnd,blockRow.FirstDataStart,blockRow.FirstDataEnd,blockRow.LastDataStart,blockRow.LastDataEnd)
			var data [][]byte
			if blockRow.ColumnStart == 1 && blockRow.ColumnEnd == int16(len(blockRow.Row.Data)) {
				data = blockRow.Row.Data
			}else if blockRow.FirstDataStart == 0 && blockRow.LastDataStart == 0 {
				data = blockRow.Row.Data[blockRow.ColumnStart-1:blockRow.ColumnEnd]
			}else{
				//fmt.Println(blockRow.ColumnStart,blockRow.ColumnEnd)
				temp := blockRow.Row.Data[blockRow.ColumnStart-1:blockRow.ColumnEnd]
				data = make([][]byte, 0, len(temp))
				data = append(data, temp...)
				if blockRow.FirstDataStart > 0 {
					data[0] = data[0][blockRow.FirstDataStart-1:blockRow.FirstDataEnd]
				}
				if blockRow.LastDataStart > 0 {
					data[len(data)-1] = data[len(data)-1][blockRow.LastDataStart-1:blockRow.LastDataEnd]
				}
			}
			rows = append(rows, db.RowData{Id:blockRow.Row.Id,Op:blockRow.Row.Op,Data:data})

			//过滤重复行并添加索引
			_,exists := rowIDMap[blockRow.Row.Id]
			if !exists {//过滤重复行
				rowIDMap[blockRow.Row.Id] = id
				if err := service.addIndex(table, id, blockRow.Row); err != nil {
					return err
				}
			}
		}
		block := db.BlockData{Id:id,TxData:db.TxData{TxID:txID,Time:db.Timestamp(timestamp)},Rows:rows,Join:b.Join}
		bytes,err := util.ConvertJsonBytes(block); if err != nil {
			return err
		}
		if err := service.storage.PutBlockData(service.database.Id, table.Id, id, bytes); err != nil {
			return err
		}
	}
	tally.Block = id
	return nil
}

/**
	主键、外建等索引
 */
func (service *BlockService) addIndex(table *db.TableData, blockID db.BlockID, row *db.RowData) error {
	//主键，由于需要支持记录版本，新增、修改、删除都需要记录(实际上只是更新底层索引树叶子节点数据)
	if err := service.indexService.PutPrimaryKeyIndex(service.database.Id, table, row.Id, row.Op, blockID); err != nil {
		return err
	}
	//外键，只需要在新增行时记录外键与主键关系
	if row.Op == db.ADD {
		if err := service.indexService.PutForeignKeysIndex(service.database.Id, table, row.Id, row); err != nil {
			return err
		}
	}
	//TODO 唯一索引、非聚族索引(暂时不支持)
	return nil
}

func (service *BlockService) rowTally(tally *db.TableTally, row *db.RowData) {
	if row.Op == db.ADD {
		tally.AddRow++
		if row.Id == 0 {//自增
			tally.Increment++
			row.Id = tally.Increment
		}else if row.Id > tally.Increment {//自增计数更新
			tally.Increment = row.Id
		}
	}else if row.Op == db.UPDATE {
		tally.UpdateRow++
	}else if row.Op == db.DELETE {
		tally.DelRow++
	}
}