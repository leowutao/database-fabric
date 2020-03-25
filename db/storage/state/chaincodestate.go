package state

import "github.com/hyperledger/fabric/core/chaincode/shim"

type ChainCodeState interface {
	GetStub() shim.ChaincodeStubInterface
	GetTxCache() map[string][]byte

	PrefixAddKey(prefix string, key string) string
	CompositeKey(keys... string) string

	PutOrDelKey(key string, value []byte, op Op) error
	GetKey(key string) ([]byte,error)
	GetCompositeKeyList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32) ([]string,error)

	PutOrDelData(prefix string, key string, value []byte, op Op) error
	GetDataHistory(objectTypePrefix string, keys []string, pageSize int32) ([][]byte,error)

	GetCompositeKeyData(objectTypePrefix string, objectType string, keys []string, filterVersion bool) ([]byte,error)
	PutOrDelCompositeKeyData(objectTypePrefix string, objectType string, attributes []string, value []byte, op Op) error
	GetCompositeKeyDataByVersion(objectTypePrefix string, objectType string, keys []string, versionBytes []byte) ([]byte,error)
	GetCompositeKeyDataList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool) ([][]byte,error)

	//Other Function
	GetState(collection,  key string) ([]byte,error)
	GetStateByRange(collection string,  startKey string, endKey string) ([]byte,error)
	GetStateByPartialCompositeKey(collection string,  objectType string, keys []string) ([]byte,error)
}

