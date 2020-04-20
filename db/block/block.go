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
	useSize = maxSize-keySize-blockSize
	rowSize = 25
)

func (service *BlockService) QueryRowBlockID(table *db.TableData, rowID db.RowID) (db.BlockID,error) {
	return service.indexService.GetPrimaryKeyIndexByLast(service.database.Id, table, rowID)
}

func (service *BlockService) QueryRowIDByForeignKey(tableID db.TableID, foreignKey db.ForeignKey, referenceRowID db.RowID) ([]db.RowID,error) {
	return service.indexService.GetForeignKeyIndex(service.database.Id, tableID, foreignKey, referenceRowID)
}

func (service *BlockService) QueryRowData(table *db.TableData, rowID db.RowID) (*db.RowData,error) {
	blockID,err := service.QueryRowBlockID(table, rowID); if err != nil {
		return nil,err
	}
	if blockID == 0 {
		return nil,err
	}
	return service.getRowData(table, blockID, rowID)
}

func (service *BlockService) joinRowData(row *db.RowData, splitRow *db.RowData, splitPosition int16) {
	index := splitPosition-1
	row.Data[index] = append(row.Data[index], splitRow.Data[0]...)
}

func (service *BlockService) getRowData(table *db.TableData, blockID db.BlockID, rowID db.RowID) (*db.RowData,error) {
	bytes,err := service.storage.GetBlockData(service.database.Id, table.Id, blockID); if err != nil {
		return nil,err
	}
	if len(bytes) == 0 {
		return nil,fmt.Errorf("block `%d` is not found", blockID)
	}
	var block db.BlockData
	if err := json.Unmarshal(bytes, &block); err != nil {
		return nil,fmt.Errorf("block `%d` convert error `%s`", blockID, err.Error())
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
	if len(block.Rows) == (rowIndex+1) && block.SplitPosition > 0 {
		splitRow,err := service.getRowData(table, blockID+1, rowID); if err != nil {
			return nil,err
		}
		service.joinRowData(row, splitRow, block.SplitPosition)
	}
	return row,nil
}

func (service *BlockService) splitRowData(use *int, row db.RowData, splitRows *[]db.RowData, blocks *[]db.BlockData) {
	var nextRow db.RowData
	*use = *use - rowSize
	isSplit := false
	for i,columnData := range row.Data {
		dataSize := len(columnData)
		if *use <= dataSize {
			newRow := row
			newRow.Data = newRow.Data[:i]
			nextRow = row
			nextRow.Data = nextRow.Data[i:]
			if *use < dataSize {
				left := columnData[:*use]
				right := columnData[*use:]
				newRow.Data = append(newRow.Data, left)
				nextRow.Data = append(nextRow.Data, right)
			}
			*splitRows = append(*splitRows, newRow)
			*blocks = append(*blocks, db.BlockData{Rows:*splitRows, SplitPosition:int16(i+1)})
			splitRows = nil
			*use = useSize
			if len(nextRow.Data) > 0 {
				service.splitRowData(use, nextRow,nil, blocks)
			}
			isSplit = true
			break
		}else{
			*use = *use - dataSize
		}
	}
	if !isSplit {
		*splitRows = append(*splitRows, row)
	}
}

func (service *BlockService) SetBlockData(table *db.TableData, rows []*db.RowData) error {
	txID,timestamp,err := service.storage.GetTxID(); if err != nil {
		return err
	}
	use := useSize
	var splitRows []db.RowData
	var blocks []db.BlockData
	for _,row := range rows {
		service.rowTally(table, row.Id, row.Op)
		service.splitRowData(&use, *row, &splitRows, &blocks)
	}
	if len(splitRows) > 0 {
		blocks = append(blocks, db.BlockData{Rows:splitRows})
	}
	id := table.Tally.Block
	rowIDMap := map[db.RowID]db.BlockID{}
	for _,block := range blocks {
		id++
		block.Id = id
		block.TxID = txID
		block.Timestamp = timestamp
		bytes,err := util.ConvertJsonBytes(block); if err != nil {
			return err
		}
		if err := service.storage.PutBlockData(service.database.Id, table.Id, id, bytes); err != nil {
			return err
		}

		//过滤重复行并添加索引
		for _,row := range block.Rows {
			_,exists := rowIDMap[row.Id]
			if !exists {//过滤重复行
				rowIDMap[row.Id] = block.Id
				if err := service.addIndex(table, block.Id, &row); err != nil {
					return err
				}
			}
		}
	}
	table.Tally.Block = id
	return nil
}

/**
	主键、外建等索引
 */
func (service *BlockService) addIndex(table *db.TableData, blockID db.BlockID, row *db.RowData) error {
	//主键，由于需要支持记录版本，新增、修改、删除都需要记录(实际上只是更新底层索引树叶子节点数据)
	if err := service.indexService.PutPrimaryKeyIndex(service.database.Id, table, row.Id, blockID); err != nil {
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

func (service *BlockService) rowTally(table *db.TableData, increment db.RowID, op db.OpType) {
	if op == db.ADD {
		table.Tally.AddRow = table.Tally.AddRow + 1
		table.Tally.Row = table.Tally.Row + 1
		if increment > table.Tally.Increment {
			table.Tally.Increment = increment
		}
	}else if op == db.UPDATE {
		table.Tally.UpdateRow = table.Tally.UpdateRow + 1
		if increment > table.Tally.Increment {
			table.Tally.Increment = increment
		}
	}else if op == db.DELETE {
		table.Tally.DelRow = table.Tally.DelRow + 1
		table.Tally.Row = table.Tally.Row - 1
	}
}