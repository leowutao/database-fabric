// Code generated by protoc-gen-go. DO NOT EDIT.
// source: row.proto

package protos

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type BlockData_JoinType int32

const (
	//不连接
	BlockData_JoinTypeNone BlockData_JoinType = 0
	//(存在分裂，记录行列表最后一条行部分数据包含在下一个块中)
	//行值连接
	BlockData_JoinTypeRow BlockData_JoinType = 1
	//列值连接
	BlockData_JoinTypeColumn BlockData_JoinType = 2
)

var BlockData_JoinType_name = map[int32]string{
	0: "JoinTypeNone",
	1: "JoinTypeRow",
	2: "JoinTypeColumn",
}

var BlockData_JoinType_value = map[string]int32{
	"JoinTypeNone":   0,
	"JoinTypeRow":    1,
	"JoinTypeColumn": 2,
}

func (x BlockData_JoinType) String() string {
	return proto.EnumName(BlockData_JoinType_name, int32(x))
}

func (BlockData_JoinType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_dbfce2cce8f2e8cd, []int{0, 0}
}

type BlockData struct {
	Id                   int32              `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	TxID                 string             `protobuf:"bytes,2,opt,name=txID,proto3" json:"txID,omitempty"`
	Time                 int64              `protobuf:"varint,3,opt,name=time,proto3" json:"time,omitempty"`
	Rows                 []*RowData         `protobuf:"bytes,4,rep,name=rows,proto3" json:"rows,omitempty"`
	Join                 BlockData_JoinType `protobuf:"varint,5,opt,name=join,proto3,enum=protos.BlockData_JoinType" json:"join,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *BlockData) Reset()         { *m = BlockData{} }
func (m *BlockData) String() string { return proto.CompactTextString(m) }
func (*BlockData) ProtoMessage()    {}
func (*BlockData) Descriptor() ([]byte, []int) {
	return fileDescriptor_dbfce2cce8f2e8cd, []int{0}
}

func (m *BlockData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BlockData.Unmarshal(m, b)
}
func (m *BlockData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BlockData.Marshal(b, m, deterministic)
}
func (m *BlockData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BlockData.Merge(m, src)
}
func (m *BlockData) XXX_Size() int {
	return xxx_messageInfo_BlockData.Size(m)
}
func (m *BlockData) XXX_DiscardUnknown() {
	xxx_messageInfo_BlockData.DiscardUnknown(m)
}

var xxx_messageInfo_BlockData proto.InternalMessageInfo

func (m *BlockData) GetId() int32 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *BlockData) GetTxID() string {
	if m != nil {
		return m.TxID
	}
	return ""
}

func (m *BlockData) GetTime() int64 {
	if m != nil {
		return m.Time
	}
	return 0
}

func (m *BlockData) GetRows() []*RowData {
	if m != nil {
		return m.Rows
	}
	return nil
}

func (m *BlockData) GetJoin() BlockData_JoinType {
	if m != nil {
		return m.Join
	}
	return BlockData_JoinTypeNone
}

type RowData struct {
	Id                   int64         `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Op                   uint32        `protobuf:"varint,2,opt,name=op,proto3" json:"op,omitempty"`
	Columns              []*ColumnData `protobuf:"bytes,3,rep,name=columns,proto3" json:"columns,omitempty"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *RowData) Reset()         { *m = RowData{} }
func (m *RowData) String() string { return proto.CompactTextString(m) }
func (*RowData) ProtoMessage()    {}
func (*RowData) Descriptor() ([]byte, []int) {
	return fileDescriptor_dbfce2cce8f2e8cd, []int{1}
}

func (m *RowData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RowData.Unmarshal(m, b)
}
func (m *RowData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RowData.Marshal(b, m, deterministic)
}
func (m *RowData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RowData.Merge(m, src)
}
func (m *RowData) XXX_Size() int {
	return xxx_messageInfo_RowData.Size(m)
}
func (m *RowData) XXX_DiscardUnknown() {
	xxx_messageInfo_RowData.DiscardUnknown(m)
}

var xxx_messageInfo_RowData proto.InternalMessageInfo

func (m *RowData) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *RowData) GetOp() uint32 {
	if m != nil {
		return m.Op
	}
	return 0
}

func (m *RowData) GetColumns() []*ColumnData {
	if m != nil {
		return m.Columns
	}
	return nil
}

type ColumnData struct {
	Data                 []byte   `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ColumnData) Reset()         { *m = ColumnData{} }
func (m *ColumnData) String() string { return proto.CompactTextString(m) }
func (*ColumnData) ProtoMessage()    {}
func (*ColumnData) Descriptor() ([]byte, []int) {
	return fileDescriptor_dbfce2cce8f2e8cd, []int{2}
}

func (m *ColumnData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ColumnData.Unmarshal(m, b)
}
func (m *ColumnData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ColumnData.Marshal(b, m, deterministic)
}
func (m *ColumnData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ColumnData.Merge(m, src)
}
func (m *ColumnData) XXX_Size() int {
	return xxx_messageInfo_ColumnData.Size(m)
}
func (m *ColumnData) XXX_DiscardUnknown() {
	xxx_messageInfo_ColumnData.DiscardUnknown(m)
}

var xxx_messageInfo_ColumnData proto.InternalMessageInfo

func (m *ColumnData) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterEnum("protos.BlockData_JoinType", BlockData_JoinType_name, BlockData_JoinType_value)
	proto.RegisterType((*BlockData)(nil), "protos.BlockData")
	proto.RegisterType((*RowData)(nil), "protos.RowData")
	proto.RegisterType((*ColumnData)(nil), "protos.ColumnData")
}

func init() { proto.RegisterFile("row.proto", fileDescriptor_dbfce2cce8f2e8cd) }

var fileDescriptor_dbfce2cce8f2e8cd = []byte{
	// 256 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x4c, 0x90, 0x31, 0x4f, 0xc3, 0x30,
	0x10, 0x85, 0xb1, 0x9d, 0x52, 0x72, 0x2d, 0x69, 0x74, 0x93, 0xc5, 0x64, 0x85, 0xc5, 0x03, 0xca,
	0x50, 0x7e, 0x01, 0xd0, 0x05, 0x06, 0x06, 0x0b, 0x89, 0x39, 0x34, 0x19, 0x0c, 0x6d, 0x2e, 0x4a,
	0x82, 0x0c, 0xbf, 0x95, 0x3f, 0x83, 0x7a, 0xc1, 0xa5, 0x53, 0x9e, 0x5e, 0x9e, 0xdf, 0xdd, 0x77,
	0x90, 0xf6, 0x14, 0xca, 0xae, 0xa7, 0x91, 0xf0, 0x9c, 0x3f, 0x43, 0xf1, 0x23, 0x20, 0xbd, 0xdf,
	0xd1, 0xf6, 0x63, 0x53, 0x8d, 0x15, 0x66, 0x20, 0x7d, 0xad, 0x85, 0x11, 0x76, 0xe6, 0xa4, 0xaf,
	0x11, 0x21, 0x19, 0xbf, 0x1e, 0x37, 0x5a, 0x1a, 0x61, 0x53, 0xc7, 0x9a, 0x3d, 0xbf, 0x6f, 0xb4,
	0x32, 0xc2, 0x2a, 0xc7, 0x1a, 0xaf, 0x21, 0xe9, 0x29, 0x0c, 0x3a, 0x31, 0xca, 0x2e, 0xd6, 0xab,
	0x69, 0xc6, 0x50, 0x3a, 0x0a, 0x87, 0x5a, 0xc7, 0x3f, 0xb1, 0x84, 0xe4, 0x9d, 0x7c, 0xab, 0x67,
	0x46, 0xd8, 0x6c, 0x7d, 0x15, 0x43, 0xc7, 0xe9, 0xe5, 0x13, 0xf9, 0xf6, 0xe5, 0xbb, 0x6b, 0x1c,
	0xe7, 0x8a, 0x3b, 0xb8, 0x88, 0x0e, 0xe6, 0xb0, 0x8c, 0xfa, 0x99, 0xda, 0x26, 0x3f, 0xc3, 0x15,
	0x2c, 0x8e, 0x79, 0x0a, 0xb9, 0x40, 0x84, 0x2c, 0x1a, 0x0f, 0xb4, 0xfb, 0xdc, 0xb7, 0xb9, 0x2c,
	0x5e, 0x61, 0xfe, 0xb7, 0xc3, 0x09, 0x9a, 0x62, 0xb4, 0x0c, 0x24, 0x75, 0x0c, 0x76, 0xe9, 0x24,
	0x75, 0x78, 0x03, 0xf3, 0x2d, 0x3f, 0x1b, 0xb4, 0x62, 0x0a, 0x8c, 0x0b, 0x4e, 0x6d, 0x0c, 0x12,
	0x23, 0x85, 0x01, 0xf8, 0xb7, 0x0f, 0x27, 0xa9, 0xab, 0xb1, 0xe2, 0xf6, 0xa5, 0x63, 0xfd, 0x36,
	0x1d, 0xf8, 0xf6, 0x37, 0x00, 0x00, 0xff, 0xff, 0x52, 0x20, 0xa4, 0x4e, 0x74, 0x01, 0x00, 0x00,
}