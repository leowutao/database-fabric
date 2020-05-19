package block

import (
	"fmt"
	"gitee.com/bidpoc/database-fabric-cc/db"
	"gitee.com/bidpoc/database-fabric-cc/db/index"
	"gitee.com/bidpoc/database-fabric-cc/db/protos"
	"gitee.com/bidpoc/database-fabric-cc/db/storage"
	"gitee.com/bidpoc/database-fabric-cc/db/storage/state"
	"github.com/golang/protobuf/proto"
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
	Row *protos.RowData `json:"row"`//行数据指针
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
	Join protos.BlockData_JoinType `json:"join"`//与下一个块连接方式
}

func (service *BlockService) QueryRowBlockID(table *db.TableData, rowID db.RowID) (db.BlockID,error) {
	return service.indexService.GetPrimaryKeyIndex(service.database.Id, table, rowID)
}

func (service *BlockService) QueryRowIDByForeignKey(tableID db.TableID, foreignKey db.ForeignKey, referenceRowID db.RowID, size int32) ([]db.RowID,error) {
	return service.indexService.GetForeignKeyIndex(service.database.Id, tableID, foreignKey, referenceRowID, size)
}

func (service *BlockService) QueryRowDataByRange(table *db.TableData, start db.RowID, end db.RowID, order db.OrderType, size int32) ([]*protos.RowData,error) {
	rowBlockIDList,err := service.indexService.GetPrimaryKeyIndexByRange(service.database.Id, table, start, end, order, size); if err != nil {
		return nil,err
	}
	rows := make([]*protos.RowData, 0, len(rowBlockIDList))
	for _,rowBlockID := range rowBlockIDList {
		if rowBlockID.BlockID == 0 {
			rows = append(rows, &protos.RowData{Id:rowBlockID.RowID})
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
			rows = append(rows, &db.RowDataHistory{Row:&protos.RowData{Id:rowID}})
		}else{
			row,err := service.getRowDataHistory(table.Id, blockID, rowID); if err != nil {
				return nil,total,err
			}
			rows = append(rows, row)
		}
	}
	return rows,total,nil
}

func (service *BlockService) QueryRowData(table *db.TableData, rowID db.RowID) (*protos.RowData,error) {
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
	row := service.initRowData(rowID)
	err = service.joinBlockRowData(tableID, blockID, row, block); if err != nil {
		return nil,err
	}
	return &db.RowDataHistory{TxID:block.TxID,Time:block.Time,Row:row},nil
}

func (service *BlockService) getRowData(tableID db.TableID, blockID db.BlockID, rowID db.RowID) (*protos.RowData,error) {
	row := service.initRowData(rowID)
	err := service.joinBlockRowData(tableID, blockID, row,nil); if err != nil {
		return nil,err
	}
	return row,nil
}

func (service *BlockService) initRowData(rowID db.RowID) *protos.RowData {
	row := &protos.RowData{Id:rowID}
	row.Columns = make([]*protos.ColumnData, 0, 0)
	return row
}

func (service *BlockService) getBlockData(tableID db.TableID, blockID db.BlockID) (*protos.BlockData,error) {
	bytes,err := service.storage.GetBlockData(service.database.Id, tableID, blockID); if err != nil {
		return nil,err
	}
	if len(bytes) == 0 {
		return nil,fmt.Errorf("block `%d` is not found", blockID)
	}
	block := &protos.BlockData{}
	if err := proto.Unmarshal(bytes, block); err != nil {
		return nil,fmt.Errorf("block `%d` convert error `%s`", blockID, err.Error())
	}
	return block,nil
}

func (service *BlockService) joinRowData(row *protos.RowData, joinRow *protos.RowData, joinType protos.BlockData_JoinType, index *int) {
	if joinType == protos.BlockData_JoinTypeRow {
		*index++
		for i,columnData := range joinRow.Columns {
			p := *index+i
			row.Columns[p].Data = append(row.Columns[p].Data, columnData.Data...)
		}
		*index = *index + len(joinRow.Columns)-1
	}else if joinType == protos.BlockData_JoinTypeColumn {
		row.Columns[*index].Data = append(row.Columns[*index].Data, joinRow.Columns[0].Data...)
		if len(joinRow.Columns) > 1 {
			for i,columnData := range joinRow.Columns[1:] {
				p := *index+i+1
				row.Columns[p].Data = append(row.Columns[p].Data, columnData.Data...)
			}
			*index = *index + len(joinRow.Columns)-1
		}
	}
}

func (service *BlockService) joinBlockRowData(tableID db.TableID, blockID db.BlockID, row *protos.RowData, firstBlock *protos.BlockData) error {
	var block *protos.BlockData
	var joinRows []*protos.RowData
	var joinTypes []protos.BlockData_JoinType
	columnLenMap := map[int]int{}
	columnIndex := 0
	for {
		if firstBlock == nil || firstBlock.Id == 0 {
			var err error
			block,err = service.getBlockData(tableID, blockID); if err != nil {
				return err
			}
		}else{
			block = firstBlock
			firstBlock = nil
		}
		rowIndex := -1
		for i:=0;i<len(block.Rows);i++ {
			if rowIndex < 0 && block.Rows[i].Id == row.Id {
				rowIndex = i
			}else{
				block.Rows[i] = nil
			}
		}
		if rowIndex < 0 {
			return fmt.Errorf("row `%d` is not found in block `%d`", blockID, row.Id)
		}
		joinRow := block.Rows[rowIndex]
		joinRows = append(joinRows, joinRow)
		joinTypes = append(joinTypes, block.Join)

		for _,columnData := range joinRow.Columns {
			columnLen := columnLenMap[columnIndex]
			columnLenMap[columnIndex] = columnLen + len(columnData.Data)
			columnIndex++
		}
		if block.Join == protos.BlockData_JoinTypeColumn {
			columnIndex--
		}
		if len(block.Rows) != (rowIndex+1) || block.Join == protos.BlockData_JoinTypeNone {//查找的行是块中最后一条并且块需要连接到下个块
			break
		}
		blockID++
	}
	row.Columns = make([]*protos.ColumnData, len(columnLenMap))
	for i:=0;i<len(row.Columns);i++ {
		row.Columns[i] = &protos.ColumnData{Data:make([]byte, 0, columnLenMap[i])}
	}
	index := -1
	for i,joinRow := range joinRows {
		join := protos.BlockData_JoinTypeRow
		if i > 0 {
			join = joinTypes[i-1]
		}
		service.joinRowData(row, joinRow, join, &index)
	}
	return nil
}

func (service *BlockService) combineRowData(use *int64, row *protos.RowData, combineRows *[]BlockRowData, blocks *[]BlockData) {
	current := int16(1)
	end := int16(len(row.Columns))
	temp := BlockRowData{Row:row,ColumnStart:current}
	for ;current<=end;current++ {
		if *use <= 2*rowSize {
			if len(*combineRows) > 0 {
				*blocks = append(*blocks, BlockData{Rows:*combineRows})
				*combineRows = nil
			}
			*use = useSize
		}
		*use = *use - rowSize
		size := int64(len(row.Columns[current-1].Data))
		if *use == size {
			blockRow := temp
			blockRow.ColumnEnd = current
			*combineRows = append(*combineRows, blockRow)
			*blocks = append(*blocks, BlockData{Rows:*combineRows,Join:protos.BlockData_JoinTypeRow})
			if current == end {//行最后一列已经计算完成，此块无需连接到下一个块
				(*blocks)[len(*blocks)-1].Join = protos.BlockData_JoinTypeNone
			}else{
				temp = BlockRowData{Row:row,ColumnStart:current+1}
			}
			*combineRows = nil
			*use = useSize
		}else if *use < size {
			blockRow := temp
			blockRow.ColumnEnd = current
			blockRow.LastDataStart = 1
			blockRow.LastDataEnd = *use
			*combineRows = append(*combineRows, blockRow)
			*blocks = append(*blocks, BlockData{Rows:*combineRows,Join:protos.BlockData_JoinTypeColumn})
			*combineRows = nil
			temp = BlockRowData{Row:row,ColumnStart:current}

			currentSize := size - *use
			cap := useSize+rowSize
			count := currentSize/cap
			have := currentSize%cap
			//fmt.Println(currentSize,cap,count,have)
			for i:=int64(0);i<count;i++{
				blockRowLoop := temp
				blockRowLoop.ColumnEnd = current
				blockRowLoop.LastDataStart = *use+cap*i+1
				blockRowLoop.LastDataEnd = *use+cap*(i+1)
				join := protos.BlockData_JoinTypeColumn
				if i == count-1 && have == 0 {
					join = protos.BlockData_JoinTypeRow
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

func (service *BlockService) SetBlockData(table *db.TableData, tally *db.TableTally, rows []*protos.RowData) error {
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
		rows := make([]*protos.RowData, 0, len(b.Rows))
		for _,blockRow := range b.Rows {
			//fmt.Println(blockRow.ColumnStart,blockRow.ColumnEnd,blockRow.FirstDataStart,blockRow.FirstDataEnd,blockRow.LastDataStart,blockRow.LastDataEnd)
			var columns []*protos.ColumnData
			isSplit := blockRow.FirstDataStart > 0 || blockRow.LastDataStart > 0
			if !isSplit && blockRow.ColumnStart == 1 && blockRow.ColumnEnd == int16(len(blockRow.Row.Columns)) {
				columns = blockRow.Row.Columns
			}else if !isSplit {
				columns = blockRow.Row.Columns[blockRow.ColumnStart-1:blockRow.ColumnEnd]
			}else{
				//fmt.Println(blockRow.ColumnStart,blockRow.ColumnEnd)
				splitColumns := blockRow.Row.Columns[blockRow.ColumnStart-1:blockRow.ColumnEnd]
				columns = make([]*protos.ColumnData, 0, len(splitColumns))
				for _,columnData := range splitColumns {
					temp := columnData.Data
					columns = append(columns, &protos.ColumnData{Data:temp})
				}
				if blockRow.FirstDataStart > 0 {
					columns[0].Data = columns[0].Data[blockRow.FirstDataStart-1:blockRow.FirstDataEnd]
				}
				if blockRow.LastDataStart > 0 {
					columns[len(columns)-1].Data = columns[len(columns)-1].Data[blockRow.LastDataStart-1:blockRow.LastDataEnd]
				}
			}
			//fmt.Print("len ")
			//for _,c := range columns {
			//	fmt.Printf("%d,",len(c.Data))
			//}
			//fmt.Print("\n")
			rows = append(rows, &protos.RowData{Id:blockRow.Row.Id,Op:blockRow.Row.Op,Columns:columns})

			//过滤重复行并添加索引
			_,exists := rowIDMap[blockRow.Row.Id]
			if !exists {//过滤重复行
				rowIDMap[blockRow.Row.Id] = id
				if err := service.addIndex(table, id, blockRow.Row); err != nil {
					return err
				}
			}
		}
		block := &protos.BlockData{Id:id,TxID:txID,Time:timestamp,Rows:rows,Join:b.Join}
		bytes,err := proto.Marshal(block); if err != nil {
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
func (service *BlockService) addIndex(table *db.TableData, blockID db.BlockID, row *protos.RowData) error {
	//主键，由于需要支持记录版本，新增、修改、删除都需要记录(实际上只是更新底层索引树叶子节点数据)
	if err := service.indexService.PutPrimaryKeyIndex(service.database.Id, table, row.Id, uint8(row.Op), blockID); err != nil {
		return err
	}
	//外键，只需要在新增行时记录外键与主键关系
	if uint8(row.Op) == db.ADD {
		if err := service.indexService.PutForeignKeysIndex(service.database.Id, table, row.Id, row); err != nil {
			return err
		}
	}
	//TODO 唯一索引、非聚族索引(暂时不支持)
	return nil
}

func (service *BlockService) rowTally(tally *db.TableTally, row *protos.RowData) {
	if uint8(row.Op) == db.ADD {
		tally.AddRow++
		if row.Id == 0 {//自增
			tally.Increment++
			row.Id = tally.Increment
		}else if row.Id > tally.Increment {//自增计数更新
			tally.Increment = row.Id
		}
	}else if uint8(row.Op) == db.UPDATE {
		tally.UpdateRow++
	}else if uint8(row.Op) == db.DELETE {
		tally.DelRow++
	}
}