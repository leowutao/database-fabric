package call

import (
	"github.com/bidpoc/database-fabric-cc/db/storage/state"
	"github.com/bidpoc/database-fabric-cc/protos/call"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

func Invoke(stub shim.ChaincodeStubInterface, protoBuf []byte) pb.Response {
	callInfo := &call.CallInfo{}
	err := proto.Unmarshal(protoBuf, callInfo); if err != nil {
		return shim.Error(err.Error())
	}
	state := state.NewStateImpl(stub)//默认合约状态结构实现，如果需要自定义可以实现state.ChainCodeState接口
	return Operation(state, callInfo)
}


func Operation(state state.ChainCodeState, callInfo *call.CallInfo) pb.Response {
	//TODO 操作序列化待实现
	switch callInfo.Type {
		case call.CallType_QUERY_DATABASE:
		case call.CallType_CREATE_DATABASE:
		case call.CallType_UPDATE_DATABASE:
		case call.CallType_DROP_DATABASE:
		case call.CallType_QUERY_TABLE:
		case call.CallType_CREATE_TABLE:
		case call.CallType_ALTER_TABLE:
		case call.CallType_DROP_TABLE:
		case call.CallType_QUERY_ROW:
		case call.CallType_QUERY_PAGINATION_ROW:
		case call.CallType_INSERT_ROW:
		case call.CallType_UPDATE_ROW:
		case call.CallType_DELETE_ROW:
		default:
			return shim.Error("call type error")
	}
	return shim.Success(nil)
}