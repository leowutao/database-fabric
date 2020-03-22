package db

import (
	"bytes"
	"encoding/json"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

/////////////////// ChainCode State Put and Get ///////////////////
const HistoryCompositeKey = "HISTORY{KEY}VERSION"

type Op int8
const (
	Set Op = iota
	Del
	SetOrDel
)

type CompositeOp int8
const (
	CompositeAll CompositeOp = iota
	CompositeKey
	CompositeValue
)

type History struct {
	Op Op `json:"op"`
	TxID string `json:"txID"`
	Timestamp int64 `json:"timestamp"`
	Value []byte `json:"value"`
}

type HistoryVersion struct {
	Version string `json:"version"`
	Total int64 `json:"total"`
}

type KV struct {
	Key  string
	Value []byte
}

////// State Cache //////
func (t *DbManager) putCache(key string, value []byte)  {
	t.CacheData[key] = value
}

func (t *DbManager) getCache(key string) []byte {
	return t.CacheData[key]
}

func (t *DbManager) getCacheValue(key string, value []byte) []byte {
	cacheValue := t.getCache(key)
	if cacheValue != nil && len(cacheValue) > 0 {
		return cacheValue
	}else{
		t.putCache(key, value)
		return value
	}
}

func (t *DbManager) delCache(key string)  {
	delete(t.CacheData, key)
}

////// State Function //////
func (t *DbManager) putData(collection, key string, value []byte) error {
	t.putCache(key, value)
	if collection == "" {
		return t.ChainCodeStub.PutState(key, value)
	}else{
		return t.ChainCodeStub.PutPrivateData(collection, key, value)
	}
}

func (t *DbManager) delData(collection, key string) error {
	t.delCache(key)
	if collection == "" {
		return t.ChainCodeStub.DelState(key)
	}else{
		return t.ChainCodeStub.DelPrivateData(collection, key)
	}
}

func (t *DbManager) getData(collection, key string) ([]byte,error) {
	var value []byte
	var err error
	value = t.getCache(key)
	if value != nil && len(value) > 0 {
		return value,nil
	}
	if collection == "" {
		value,err = t.ChainCodeStub.GetState(key)
	}else{
		value,err = t.ChainCodeStub.GetPrivateData(collection, key)
	}
	if err !=nil {
		return nil,err
	}
	if value != nil && len(value) > 0 {
		t.putCache(key, value)
	}
	return value,nil
}

func (t *DbManager) getDataByRangeIterator(collection, startKey, endKey string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return t.ChainCodeStub.GetStateByRange(startKey, endKey)
	}else{
		return t.ChainCodeStub.GetPrivateDataByRange(collection, startKey, endKey)
	}
}

func (t *DbManager) getDataByPartialCompositeKeyIterator(collection, objectType string, keys []string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return t.ChainCodeStub.GetStateByPartialCompositeKey(objectType, keys)
	}else{
		return t.ChainCodeStub.GetPrivateDataByPartialCompositeKey(collection, objectType, keys)
	}
}

func (t *DbManager) getPaginationDataList(resultsIterator shim.StateQueryIteratorInterface, pageSize int32) ([]KV, error) {
	var results []KV
	defer resultsIterator.Close()
	var i int32
	pageSize = t.PageSize(pageSize)
	for i = 0; resultsIterator.HasNext(); {
		if pageSize > 0 && i >= pageSize {
			break
		}
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return nil,err
		}
		value := t.getCache(responseRange.Key)
		if value == nil || len(value) == 0 {
			value = responseRange.Value
		}
		results = append(results, KV{responseRange.Key,value})
	}
	return results,nil
}

func (t *DbManager) getDataByRange(collection, startKey, endKey string, pageSize int32) ([]KV, error) {
	resultsIterator,err := t.getDataByRangeIterator(collection, startKey, endKey)
	if err != nil {
		return nil,err
	}
	return t.getPaginationDataList(resultsIterator, pageSize)
}

func (t *DbManager) getDataByPartialCompositeKey(collection, objectType string, keys []string, pageSize int32) ([]KV, error) {
	resultsIterator,err := t.getDataByPartialCompositeKeyIterator(collection, objectType, keys)
	if err != nil {
		return nil,err
	}
	return t.getPaginationDataList(resultsIterator, pageSize)
}

////// Private Function //////
func (t *DbManager) createCompositeKey(objectType string, attributes []string) (string, error) {
	return t.ChainCodeStub.CreateCompositeKey(objectType, attributes)
}

func (t *DbManager) splitCompositeKey(compositeKey string) (string, []string, error) {
	return t.ChainCodeStub.SplitCompositeKey(compositeKey)
}

func (t *DbManager) getParameters() []string {
	_,parameters := t.ChainCodeStub.GetFunctionAndParameters()
	return parameters
}

func (t *DbManager) prefixAddKey(prefix string, key string) string {
	return prefix + key
}

func (t *DbManager) compositeKey(keys... string) string {
	compositeKey := ""
	for _,key := range keys {
		if compositeKey != "" {
			compositeKey += "~"
		}
		compositeKey += key
	}
	return compositeKey
}

func (t *DbManager) getCollectionKey() (string,error) {
	parameters := t.getParameters()
	if parameters == nil || len(parameters) == 0 {
		return "",nil
	}
	return parameters[0],nil
}

func (t *DbManager) putVersion(collection string, key string, versionKey string) error {
	value,err := t.getData(collection, key); if err !=nil {
		return err
	}
	version := HistoryVersion{versionKey, 1}
	if len(value) >0 {
		err = json.Unmarshal(value, &version); if err !=nil {
			return err
		}
		version.Total = version.Total + 1
		version.Version = versionKey
	}
	versionBytes,err := t.ConvertJsonBytes(version); if err != nil {
		return err
	}
	return t.putData(collection, key, versionBytes)
}

func (t *DbManager) putDataHistory(collection string, objectTypePrefix string, attributes []string, value []byte, op Op) (string,error) {
	versionKey := ""
	timestamp,err := t.ChainCodeStub.GetTxTimestamp(); if err != nil {
		return versionKey,err
	}
	seconds := t.Int64ToString(timestamp.Seconds)
	history := History{op,t.ChainCodeStub.GetTxID(),timestamp.Seconds,value}
	bytes,err := t.ConvertJsonBytes(history); if err != nil {
		return versionKey,err
	}
	versionKey = seconds
	txID := t.ChainCodeStub.GetTxID()
	if txID != "" {
		txSub := ""
		txIDLen := len(txID)
		if txIDLen > 8 {
			txSub = txID[0 : 4]
			txSub += txID[txIDLen - 4 : txIDLen]
		}else{
			txSub = txID
		}
		versionKey +=txSub
	}

	attributes = append(attributes, versionKey)
	historyKey, err := t.createCompositeKey(t.prefixAddKey(objectTypePrefix, HistoryCompositeKey), attributes)
	if err != nil {
		return versionKey,err
	}
	return versionKey,t.putData(collection, historyKey, bytes)
}

func (t *DbManager) getVersion(collection string, key string) ([]byte,error) {
	return t.getData(collection, key)
}

func (t *DbManager) getDataByVersion(collection string, key string, objectTypePrefix string, attributes []string) ([]byte,error) {
	versionBytes,err := t.getData(collection, key); if err !=nil {
		return nil,err
	}
	return t.getDataByVersionBytes(collection, objectTypePrefix, attributes, versionBytes)
}

func (t *DbManager) getDataByVersionBytes(collection string, objectTypePrefix string, attributes []string, versionBytes []byte) ([]byte,error) {
	if len(versionBytes) >0 {
		version := HistoryVersion{}
		err := json.Unmarshal(versionBytes, &version); if err !=nil {
			return nil,err
		}
		attributes = append(attributes, version.Version)
		key,err := t.createCompositeKey(t.prefixAddKey(objectTypePrefix, HistoryCompositeKey), attributes)
		historyBytes,err := t.getData(collection, key); if err !=nil {
			return nil,err
		}
		history := History{}
		err = json.Unmarshal(historyBytes, &history); if err !=nil {
			return nil,err
		}
		return history.Value,nil
	}else{
		return nil,nil
	}
}

func (t *DbManager) getCompositeList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool, op CompositeOp) ([]string,[][]byte,error) {
	var compositeKeys []string
	var compositeValues [][]byte
	collection,err := t.getCollectionKey()
	if err != nil {
		return nil,nil,err
	}
	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	resultsIterator,err := t.getDataByPartialCompositeKeyIterator(collection, objectType, prefixKeys)
	if err != nil {
		return nil,nil,err
	}
	defer resultsIterator.Close()
	var i int32
	pageSize = t.PageSize(pageSize)
	match := false
	for i = 0; resultsIterator.HasNext(); {
		if pageSize > 0 && i >= pageSize {
			break
		}
		if err != nil {
			return nil,nil,err
		}
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return nil,nil,err
		}
		_,compositeKeyParts, err := t.splitCompositeKey(responseRange.Key)
		if err != nil {
			return nil,nil,err
		}

		if !match && len(keys) > 0 {
			match = true
			for j,key := range keys {
				index := j+len(prefixKeys)
				if index >= len(compositeKeyParts) || key != compositeKeyParts[index] {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		i++

		value := t.getCacheValue(responseRange.Key, responseRange.Value)

		if op == CompositeKey || op == CompositeAll {
			index := len(keys) + len(prefixKeys)
			if index >= len(compositeKeyParts) {
				index = len(compositeKeyParts)
			}
			compositeKeys = append(compositeKeys, compositeKeyParts[index])
		}else if op == CompositeValue || op == CompositeAll {
			if filterVersion {
				compositeValues = append(compositeValues, value)
			}else {
				value,err := t.getDataByVersionBytes(collection, objectTypePrefix, compositeKeyParts, value); if err !=nil {
					return nil,nil,err
				}
				compositeValues = append(compositeValues, value)
			}
		}
	}
	return compositeKeys,compositeValues,nil
}

////// State Operation //////
func (t *DbManager) putOrDelKey(key string, value []byte, op Op) error {
	collection,err := t.getCollectionKey()
	if err != nil {
		return err
	}
	if op == Set {
		return t.putData(collection, key, value)
	}else if op == Del {
		return t.delData(collection, key)
	}
	return nil
}

func (t *DbManager) getKey(key string) ([]byte,error) {
	collection,err := t.getCollectionKey()
	if err != nil {
		return nil,err
	}
	return t.getData(collection, key)
}

func (t *DbManager) putOrDelData(prefix string, key string, value []byte, op Op) error {
	collection,err := t.getCollectionKey()
	if err != nil {
		return err
	}
	version,err := t.putDataHistory(collection, prefix, []string{ key }, value, op); if err != nil {
		return err
	}

	return t.putVersion(collection, t.prefixAddKey(prefix, key), version)
}

func (t *DbManager) putOrDelCompositeKeyData(objectTypePrefix string, objectType string, attributes []string, value []byte, op Op) error {
	collection,err := t.getCollectionKey()
	if err != nil {
		return err
	}
	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	key, err := t.createCompositeKey(objectType, attributes)
	if err != nil {
		return err
	}
	version,err := t.putDataHistory(collection, objectTypePrefix, attributes, value, op); if err != nil {
		return err
	}

	return t.putVersion(collection, key, version)
}

func (t *DbManager) getCompositeKeyDataList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool) ([][]byte,error) {
	_,compositeValues,err :=  t.getCompositeList(objectTypePrefix, objectType, prefixKeys, keys, pageSize, filterVersion, CompositeValue); if err != nil {
		return nil,err
	}
	return compositeValues,nil
}

func (t *DbManager) getCompositeKeyList(objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32) ([]string,error) {
	compositeKeys,_,err := t.getCompositeList(objectTypePrefix, objectType, prefixKeys, keys, pageSize,false, CompositeKey); if err != nil {
		return nil,err
	}
	return compositeKeys,nil
}

func (t *DbManager) getCompositeKeyData(objectTypePrefix string, objectType string, keys []string, filterVersion bool) ([]byte,error) {
	collection,err := t.getCollectionKey()
	if err != nil {
		return nil,err
	}

	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	partialCompositeKey, err := t.createCompositeKey(objectType, keys); if err != nil {
		return nil,err
	}
	versionBytes,err := t.getData(collection, partialCompositeKey); if err !=nil {
		return nil,err
	}
	if filterVersion {
		return versionBytes,nil
	}else{
		return t.getDataByVersionBytes(collection, objectTypePrefix, keys, versionBytes)
	}
}

func (t *DbManager) getCompositeKeyDataByVersion(objectTypePrefix string, objectType string, keys []string, versionBytes []byte) ([]byte,error) {
	collection,err := t.getCollectionKey()
	if err != nil {
		return nil,err
	}

	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	return t.getDataByVersionBytes(collection, objectTypePrefix, keys, versionBytes)
}

func (t *DbManager) getDataHistory(objectTypePrefix string, keys []string, pageSize int32) ([][]byte,error) {
	return t.getCompositeKeyDataList(objectTypePrefix, HistoryCompositeKey, keys, []string{}, pageSize,true)
}

/////////////////// Other State Function ///////////////////

func (t *DbManager) GetState(collection,  key string) ([]byte,error) {
	return t.getData(collection, key)
}

func (t *DbManager) GetStateByRange(collection string,  startKey string, endKey string) ([]byte,error) {
	resultsIterator, err := t.getDataByRangeIterator(collection, startKey, endKey)
	if err != nil {
		return nil,err
	}
	return t.getStateQueryIterator(resultsIterator)
}

func (t *DbManager) GetStateByPartialCompositeKey(collection string,  objectType string, keys []string) ([]byte,error) {
	resultsIterator, err := t.getDataByPartialCompositeKeyIterator(collection, objectType, keys)
	if err != nil {
		return nil,err
	}
	return t.getStateQueryIterator(resultsIterator)
}

func (t *DbManager) getStateQueryIterator(resultsIterator shim.StateQueryIteratorInterface) ([]byte,error) {
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