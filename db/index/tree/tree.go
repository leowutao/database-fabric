package tree

type InsertType int8
const (
	InsertTypeDefault InsertType = iota
	InsertTypeReplace
	InsertTypeAppend
)

type ValueType = uint8
const (
	ValueTypeNone ValueType = iota
	ValueTypeData
	ValueTypePointer
	ValueTypeCollection
)