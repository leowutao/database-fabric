package state

import (
	"bytes"
	"github.com/database-fabric/db"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

type StateImpl struct {
	stub shim.ChaincodeStubInterface
	txCache map[string][]byte
}

func NewStateImpl(stub shim.ChaincodeStubInterface) *StateImpl {
	return &StateImpl{stub,map[string][]byte{}}
}

func (state *StateImpl) GetStub() shim.ChaincodeStubInterface {
	return state.stub
}

func (state *StateImpl) GetTxCache() map[string][]byte {
	return state.txCache
}

////// Common Operation //////
func (state *StateImpl) PrefixAddKey(prefix string, key string) string {
	return prefix + "-" + key
}

func (state *StateImpl) CompositeKey(keys... string) string {
	compositeKey := ""
	for _,key := range keys {
		if compositeKey != "" {
			compositeKey += "~"
		}
		compositeKey += key
	}
	return compositeKey
}

////// State Operation //////
func (state *StateImpl) PutOrDelKey(key string, value []byte, op db.StateType) error {
	collection,err := state.getCollectionKey()
	if err != nil {
		return err
	}
	if op == db.SetState {
		return state.putData(collection, key, value)
	}else if op == db.DelState {
		return state.delData(collection, key)
	}
	return nil
}

func (state *StateImpl) GetKey(key string) ([]byte,error) {
	collection,err := state.getCollectionKey()
	if err != nil {
		return nil,err
	}
	return state.getData(collection, key)
}

/////////////////// Other State Function ///////////////////

func (state *StateImpl) GetState(collection,  key string) ([]byte,error) {
	return state.getData(collection, key)
}

func (state *StateImpl) GetStateByRange(collection string,  startKey string, endKey string) ([]byte,error) {
	resultsIterator, err := state.getDataByRangeIterator(collection, startKey, endKey)
	if err != nil {
		return nil,err
	}
	return state.getStateQueryIterator(resultsIterator)
}

func (state *StateImpl) GetStateByPartialCompositeKey(collection string,  objectType string, keys []string) ([]byte,error) {
	resultsIterator, err := state.getDataByPartialCompositeKeyIterator(collection, objectType, keys)
	if err != nil {
		return nil,err
	}
	return state.getStateQueryIterator(resultsIterator)
}

func (state *StateImpl) getStateQueryIterator(resultsIterator shim.StateQueryIteratorInterface) ([]byte,error) {
	defer resultsIterator.Close()
	var i int32
	var buffer bytes.Buffer
	buffer.WriteString("[")
	bArrayMemberAlreadyWritten := false
	for i = 0; resultsIterator.HasNext(); i++ {
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return nil,err
		}
		if bArrayMemberAlreadyWritten == true {
			buffer.WriteString(",")
		}
		buffer.WriteString("{\"Key\":")
		buffer.WriteString("\"")
		buffer.WriteString(responseRange.Key)
		buffer.WriteString("\"")

		buffer.WriteString(", \"Value\":")
		// Record is a JSON object, so we write as-is
		buffer.WriteString(string(responseRange.Value))
		buffer.WriteString("}")
		bArrayMemberAlreadyWritten = true
	}
	buffer.WriteString("]")

	return buffer.Bytes(),nil
}

/////////////////// ChainCode State Put and Get ///////////////////
////// State Cache //////
func (state *StateImpl) putTxCache(key string, value []byte)  {
	state.txCache[key] = value
}

func (state *StateImpl) getTxCache(key string) []byte {
	return state.txCache[key]
}

func (state *StateImpl) getCacheValue(key string, value []byte) []byte {
	cacheValue := state.getTxCache(key)
	if cacheValue != nil && len(cacheValue) > 0 {
		return cacheValue
	}else{
		state.putTxCache(key, value)
		return value
	}
}

func (state *StateImpl) delTxCache(key string)  {
	delete(state.txCache, key)
}

////// State Function //////
func (state *StateImpl) putData(collection, key string, value []byte) error {
	state.putTxCache(key, value)
	if collection == "" {
		return state.stub.PutState(key, value)
	}else{
		return state.stub.PutPrivateData(collection, key, value)
	}
}

func (state *StateImpl) delData(collection, key string) error {
	state.delTxCache(key)
	if collection == "" {
		return state.stub.DelState(key)
	}else{
		return state.stub.DelPrivateData(collection, key)
	}
}

func (state *StateImpl) getData(collection, key string) ([]byte,error) {
	var value []byte
	var err error
	value = state.getTxCache(key)
	if value != nil && len(value) > 0 {
		return value,nil
	}
	if collection == "" {
		value,err = state.stub.GetState(key)
	}else{
		value,err = state.stub.GetPrivateData(collection, key)
	}
	if err !=nil {
		return nil,err
	}
	if value != nil && len(value) > 0 {
		state.putTxCache(key, value)
	}
	return value,nil
}

////// Private Function //////
func (state *StateImpl) getCollectionKey() (string,error) {
	parameters := state.getParameters()
	if parameters == nil || len(parameters) == 0 {
		return "",nil
	}
	return parameters[0],nil
}

func (state *StateImpl) getParameters() []string {
	_,parameters := state.stub.GetFunctionAndParameters()
	return parameters
}

func (state *StateImpl) getDataByRangeIterator(collection, startKey, endKey string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return state.stub.GetStateByRange(startKey, endKey)
	}else{
		return state.stub.GetPrivateDataByRange(collection, startKey, endKey)
	}
}

func (state *StateImpl) getDataByPartialCompositeKeyIterator(collection, objectType string, keys []string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return state.stub.GetStateByPartialCompositeKey(objectType, keys)
	}else{
		return state.stub.GetPrivateDataByPartialCompositeKey(collection, objectType, keys)
	}
}

/////////////////// ChainCode State Extend ///////////////////

//const HistoryCompositeKey = "HISTORY{KEY}VERSION"
//
//type CompositeOp int8
//const (
//	CompositeAll CompositeOp = iota
//	CompositeKey
//	CompositeValue
//)
//
//type History struct {
//	Op db.StateType `json:"op"`
//	TxID string `json:"txID"`
//	Timestamp int64 `json:"timestamp"`
//	Value []byte `json:"value"`
//}
//
//type HistoryVersion struct {
//	Version string `json:"version"`
//	Total int64 `json:"total"`
//}
//
//type KV struct {
//	Key  string
//	Value []byte
//}

//func (state *StateImpl) PutOrDelData(prefix string, key string, value []byte, op db.StateType) error {
//	collection,err := state.getCollectionKey()
//	if err != nil {
//		return err
//	}
//	version,err := state.putDataHistory(collection, prefix, []string{ key }, value, op); if err != nil {
//		return err
//	}
//
//	return state.putVersion(collection, state.PrefixAddKey(prefix, key), version)
//}
//
//func (state *StateImpl) PutOrDelCompositeKeyData(objectTypePrefix string, objectType string, attributes []string, value []byte, op db.StateType) error {
//	collection,err := state.getCollectionKey()
//	if err != nil {
//		return err
//	}
//	objectType = state.PrefixAddKey(objectTypePrefix, objectType)
//	key, err := state.createCompositeKey(objectType, attributes)
//	if err != nil {
//		return err
//	}
//	version,err := state.putDataHistory(collection, objectTypePrefix, attributes, value, op); if err != nil {
//		return err
//	}
//
//	return state.putVersion(collection, key, version)
//}
//
//func (state *StateImpl) GetCompositeKeyDataList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool) ([][]byte,error) {
//	_,compositeValues,err :=  state.getCompositeList(objectTypePrefix, objectType, prefixKeys, keys, pageSize, filterVersion, CompositeValue); if err != nil {
//		return nil,err
//	}
//	return compositeValues,nil
//}
//
//func (state *StateImpl) GetCompositeKeyList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32) ([]string,error) {
//	compositeKeys,_,err := state.getCompositeList(objectTypePrefix, objectType, prefixKeys, keys, pageSize,false, CompositeKey); if err != nil {
//		return nil,err
//	}
//	return compositeKeys,nil
//}
//
//func (state *StateImpl) GetCompositeKeyData(objectTypePrefix string, objectType string, keys []string, filterVersion bool) ([]byte,error) {
//	collection,err := state.getCollectionKey()
//	if err != nil {
//		return nil,err
//	}
//
//	objectType = state.PrefixAddKey(objectTypePrefix, objectType)
//	partialCompositeKey, err := state.createCompositeKey(objectType, keys); if err != nil {
//		return nil,err
//	}
//	versionBytes,err := state.getData(collection, partialCompositeKey); if err !=nil {
//		return nil,err
//	}
//	if filterVersion {
//		return versionBytes,nil
//	}else{
//		return state.getDataByVersionBytes(collection, objectTypePrefix, keys, versionBytes)
//	}
//}
//
//func (state *StateImpl) GetCompositeKeyDataByVersion(objectTypePrefix string, objectType string, keys []string, versionBytes []byte) ([]byte,error) {
//	collection,err := state.getCollectionKey()
//	if err != nil {
//		return nil,err
//	}
//
//	objectType = state.PrefixAddKey(objectTypePrefix, objectType)
//	return state.getDataByVersionBytes(collection, objectTypePrefix, keys, versionBytes)
//}
//
//func (state *StateImpl) GetDataHistory(objectTypePrefix string, keys []string, pageSize int32) ([][]byte,error) {
//	return state.GetCompositeKeyDataList(objectTypePrefix, HistoryCompositeKey, keys, []string{}, pageSize,true)
//}

////// Private Function //////
//func (state *StateImpl) createCompositeKey(objectType string, attributes []string) (string, error) {
//	return state.stub.CreateCompositeKey(objectType, attributes)
//}
//
//func (state *StateImpl) splitCompositeKey(compositeKey string) (string, []string, error) {
//	return state.stub.SplitCompositeKey(compositeKey)
//}
//
//func (state *StateImpl) getPaginationDataList(resultsIterator shim.StateQueryIteratorInterface, pageSize int32) ([]KV, error) {
//	var results []KV
//	defer resultsIterator.Close()
//	var i int32
//	pageSize = util.PageSize(pageSize)
//	for i = 0; resultsIterator.HasNext(); {
//		if pageSize > 0 && i >= pageSize {
//			break
//		}
//		responseRange, err := resultsIterator.Next()
//		if err != nil {
//			return nil,err
//		}
//		value := state.getTxCache(responseRange.Key)
//		if value == nil || len(value) == 0 {
//			value = responseRange.Value
//		}
//		results = append(results, KV{responseRange.Key,value})
//	}
//	return results,nil
//}
//
//func (state *StateImpl) getDataByRange(collection, startKey, endKey string, pageSize int32) ([]KV, error) {
//	resultsIterator,err := state.getDataByRangeIterator(collection, startKey, endKey)
//	if err != nil {
//		return nil,err
//	}
//	return state.getPaginationDataList(resultsIterator, pageSize)
//}
//
//func (state *StateImpl) getDataByPartialCompositeKey(collection, objectType string, keys []string, pageSize int32) ([]KV, error) {
//	resultsIterator,err := state.getDataByPartialCompositeKeyIterator(collection, objectType, keys)
//	if err != nil {
//		return nil,err
//	}
//	return state.getPaginationDataList(resultsIterator, pageSize)
//}
//
//////// Private Function //////
//
//func (state *StateImpl) putVersion(collection string, key string, versionKey string) error {
//	value,err := state.getData(collection, key); if err !=nil {
//		return err
//	}
//	version := HistoryVersion{versionKey, 1}
//	if len(value) >0 {
//		err = json.Unmarshal(value, &version); if err !=nil {
//			return err
//		}
//		version.Total = version.Total + 1
//		version.Version = versionKey
//	}
//	versionBytes,err := util.ConvertJsonBytes(version); if err != nil {
//		return err
//	}
//	return state.putData(collection, key, versionBytes)
//}
//
//func (state *StateImpl) putDataHistory(collection string, objectTypePrefix string, attributes []string, value []byte, op db.StateType) (string,error) {
//	versionKey := ""
//	timestamp,err := state.stub.GetTxTimestamp(); if err != nil {
//		return versionKey,err
//	}
//	seconds := util.Int64ToString(timestamp.Seconds)
//	history := History{op,state.stub.GetTxID(),timestamp.Seconds,value}
//	bytes,err := util.ConvertJsonBytes(history); if err != nil {
//		return versionKey,err
//	}
//	versionKey = seconds
//	txID := state.stub.GetTxID()
//	if txID != "" {
//		txSub := ""
//		txIDLen := len(txID)
//		if txIDLen > 8 {
//			txSub = txID[0 : 4]
//			txSub += txID[txIDLen - 4 : txIDLen]
//		}else{
//			txSub = txID
//		}
//		versionKey +=txSub
//	}
//
//	attributes = append(attributes, versionKey)
//	historyKey, err := state.createCompositeKey(state.PrefixAddKey(objectTypePrefix, HistoryCompositeKey), attributes)
//	if err != nil {
//		return versionKey,err
//	}
//	return versionKey,state.putData(collection, historyKey, bytes)
//}
//
//func (state *StateImpl) getVersion(collection string, key string) ([]byte,error) {
//	return state.getData(collection, key)
//}
//
//func (state *StateImpl) getDataByVersion(collection string, key string, objectTypePrefix string, attributes []string) ([]byte,error) {
//	versionBytes,err := state.getData(collection, key); if err !=nil {
//		return nil,err
//	}
//	return state.getDataByVersionBytes(collection, objectTypePrefix, attributes, versionBytes)
//}
//
//func (state *StateImpl) getDataByVersionBytes(collection string, objectTypePrefix string, attributes []string, versionBytes []byte) ([]byte,error) {
//	if len(versionBytes) >0 {
//		version := HistoryVersion{}
//		err := json.Unmarshal(versionBytes, &version); if err !=nil {
//			return nil,err
//		}
//		attributes = append(attributes, version.Version)
//		key,err := state.createCompositeKey(state.PrefixAddKey(objectTypePrefix, HistoryCompositeKey), attributes)
//		historyBytes,err := state.getData(collection, key); if err !=nil {
//			return nil,err
//		}
//		history := History{}
//		err = json.Unmarshal(historyBytes, &history); if err !=nil {
//			return nil,err
//		}
//		return history.Value,nil
//	}else{
//		return nil,nil
//	}
//}
//
//func (state *StateImpl) getCompositeList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool, op CompositeOp) ([]string,[][]byte,error) {
//	var compositeKeys []string
//	var compositeValues [][]byte
//	collection,err := state.getCollectionKey()
//	if err != nil {
//		return nil,nil,err
//	}
//	objectType = state.PrefixAddKey(objectTypePrefix, objectType)
//	resultsIterator,err := state.getDataByPartialCompositeKeyIterator(collection, objectType, prefixKeys)
//	if err != nil {
//		return nil,nil,err
//	}
//	defer resultsIterator.Close()
//	var i int32
//	pageSize = util.PageSize(pageSize)
//	match := false
//	for i = 0; resultsIterator.HasNext(); {
//		if pageSize > 0 && i >= pageSize {
//			break
//		}
//		if err != nil {
//			return nil,nil,err
//		}
//		responseRange, err := resultsIterator.Next()
//		if err != nil {
//			return nil,nil,err
//		}
//		_,compositeKeyParts, err := state.splitCompositeKey(responseRange.Key)
//		if err != nil {
//			return nil,nil,err
//		}
//
//		if !match && len(keys) > 0 {
//			match = true
//			for j,key := range keys {
//				index := j+len(prefixKeys)
//				if index >= len(compositeKeyParts) || key != compositeKeyParts[index] {
//					match = false
//					break
//				}
//			}
//			if !match {
//				continue
//			}
//		}
//		i++
//
//		value := state.getCacheValue(responseRange.Key, responseRange.Value)
//
//		if op == CompositeKey || op == CompositeAll {
//			index := len(keys) + len(prefixKeys)
//			if index >= len(compositeKeyParts) {
//				index = len(compositeKeyParts)
//			}
//			compositeKeys = append(compositeKeys, compositeKeyParts[index])
//		}else if op == CompositeValue || op == CompositeAll {
//			if filterVersion {
//				compositeValues = append(compositeValues, value)
//			}else {
//				value,err := state.getDataByVersionBytes(collection, objectTypePrefix, compositeKeyParts, value); if err !=nil {
//					return nil,nil,err
//				}
//				compositeValues = append(compositeValues, value)
//			}
//		}
//	}
//	return compositeKeys,compositeValues,nil
//}