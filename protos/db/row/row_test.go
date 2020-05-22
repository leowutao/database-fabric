package row

import (
	"gitee.com/bidpoc/database-fabric-cc/db"
	"github.com/golang/protobuf/proto"
	"testing"
)

func TestRow(t *testing.T) {
	columnSize := 1024*1024
	columnData := make([]byte, 0, columnSize)
	for i:=0;i<columnSize;i++{
		columnData = append(columnData,1)
	}
	size := 5000
	columns := make([]*ColumnData, 0, size)
	for i:=0;i<size;i++ {
		columns = append(columns, &ColumnData{Data:columnData})
	}
	row := &RowData{Id:db.RowID(1),Op:uint32(db.ADD),Columns:columns}
	proto.Marshal(row)
	//fmt.Println("序列化之后的信息为：",buffer)
	//newRow := &RowData{}
	//proto.Unmarshal(buffer, newRow)
	//fmt.Println("反序列化之后的信息为：",newRow)
}