package history

import (
	"github.com/bidpoc/database-fabric-cc/db"
	"github.com/bidpoc/database-fabric-cc/db/util"
	"github.com/bidpoc/database-fabric-cc/op/table"
)

type HistoryOperation struct {
	iDatabase db.DatabaseInterface
}

func NewHistoryOperation(iDatabase db.DatabaseInterface) *HistoryOperation {
	return &HistoryOperation{iDatabase}
}

////////////////// Public Function //////////////////
func (operation *HistoryOperation) QueryRowHistoryWithPaginationBytes(tableName string, rowID db.RowID, order db.OrderType, pageSize int32) ([]byte,error) {
	table,err := table.ValidateNullOfData(tableName, operation.iDatabase); if err != nil {
		return nil,err
	}
	pagination,err := operation.QueryRowHistoryWithPagination(table, rowID, order, pageSize); if err != nil {
		return nil,err
	}
	paginationBytes,err := util.ConvertJsonBytes(pagination); if err != nil {
		return nil,err
	}
	return paginationBytes,nil
}

func (operation *HistoryOperation) QueryRowHistoryWithPagination(table *db.Table, rowID db.RowID, order db.OrderType, pageSize int32) (db.Pagination,error) {
	pagination := db.Pagination{}
	rows,total,err := operation.iDatabase.QueryRowDataHistoryByRange(table.Data, rowID, order, pageSize); if err != nil {
		return pagination,err
	}
	list := make([]db.JsonData, 0, len(rows))
	for _,history := range rows {
		if history == nil {
			continue
		}
		historyJson := db.JsonData{"tx":history.TxID,"time":history.Time}
		rowData := history.Row
		if rowData != nil && rowData.Id > 0 {
			rowJson := db.JsonData{}
			if len(rowData.Columns) > 0 {
				rowJson,err = util.ParseRowData(table, rowData); if err != nil {
					return pagination,err
				}
			}else{
				rowJson[table.Primary.Name] = rowData.Id
			}
			historyJson["data"] = rowJson
		}
		list = append(list, historyJson)
	}
	return util.Pagination(pageSize, total, list),nil
}