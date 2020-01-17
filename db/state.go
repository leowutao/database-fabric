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


////// State Function //////
func (t *DbManager) putData(stub shim.ChaincodeStubInterface, collection, key string, value []byte) error {
	if collection == "" {
		return stub.PutState(key, value)
	}else{
		return stub.PutPrivateData(collection, key, value)
	}
}

func (t *DbManager) delData(stub shim.ChaincodeStubInterface, collection, key string) error {
	if collection == "" {
		return stub.DelState(key)
	}else{
		return stub.DelPrivateData(collection, key)
	}
}

func (t *DbManager) getData(stub shim.ChaincodeStubInterface, collection, key string) ([]byte,error) {
	if collection == "" {
		return stub.GetState(key)
	}else{
		return stub.GetPrivateData(collection, key)
	}
}

func (t *DbManager) getDataByRange(stub shim.ChaincodeStubInterface, collection, startKey, endKey string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return stub.GetStateByRange(startKey, endKey)
	}else{
		return stub.GetPrivateDataByRange(collection, startKey, endKey)
	}
}


func (t *DbManager) getDataByPartialCompositeKey(stub shim.ChaincodeStubInterface, collection, objectType string, keys []string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return stub.GetStateByPartialCompositeKey(objectType, keys)
	}else{
		return stub.GetPrivateDataByPartialCompositeKey(collection, objectType, keys)
	}
}

func (t *DbManager) createCompositeKey(stub shim.ChaincodeStubInterface, objectType string, attributes []string) (string, error) {
	return stub.CreateCompositeKey(objectType, attributes)
}

func (t *DbManager) splitCompositeKey(stub shim.ChaincodeStubInterface, compositeKey string) (string, []string, error) {
	return stub.SplitCompositeKey(compositeKey)
}

func (t *DbManager) getParameters(stub shim.ChaincodeStubInterface) []string {
	_,parameters := stub.GetFunctionAndParameters()
	return parameters
}

////// Private Function //////
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

func (t *DbManager) getCollectionKey(stub shim.ChaincodeStubInterface) (string,error) {
	parameters := t.getParameters(stub)
	if len(parameters) < 0 {
		return "",nil
	}
	return parameters[0],nil
}

func (t *DbManager) putVersion(stub shim.ChaincodeStubInterface, collection string, key string, versionKey string) error {
	value,err := t.getData(stub, collection, key); if err !=nil {
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
	return t.putData(stub, collection, key, versionBytes)
}

func (t *DbManager) putDataHistory(stub shim.ChaincodeStubInterface, collection string, objectTypePrefix string, attributes []string, value []byte, op Op) (string,error) {
	versionKey := ""
	timestamp,err := stub.GetTxTimestamp(); if err != nil {
		return versionKey,err
	}
	seconds := t.Int64ToString(timestamp.Seconds)
	history := History{op,stub.GetTxID(),timestamp.Seconds,value}
	bytes,err := t.ConvertJsonBytes(history); if err != nil {
		return versionKey,err
	}
	versionKey = seconds
	txID := stub.GetTxID()
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
	historyKey, err := t.createCompositeKey(stub, t.prefixAddKey(objectTypePrefix, HistoryCompositeKey), attributes)
	if err != nil {
		return versionKey,err
	}
	return versionKey,t.putData(stub, collection, historyKey, bytes)
}

func (t *DbManager) getVersion(stub shim.ChaincodeStubInterface, collection string, key string) ([]byte,error) {
	return t.getData(stub, collection, key)
}

func (t *DbManager) getDataByVersion(stub shim.ChaincodeStubInterface, collection string, key string, objectTypePrefix string, attributes []string) ([]byte,error) {
	versionBytes,err := t.getData(stub, collection, key); if err !=nil {
		return nil,err
	}
	return t.getDataByVersionBytes(stub, collection, objectTypePrefix, attributes, versionBytes)
}

func (t *DbManager) getDataByVersionBytes(stub shim.ChaincodeStubInterface, collection string, objectTypePrefix string, attributes []string, versionBytes []byte) ([]byte,error) {
	if len(versionBytes) >0 {
		version := HistoryVersion{}
		err := json.Unmarshal(versionBytes, &version); if err !=nil {
			return nil,err
		}
		attributes = append(attributes, version.Version)
		key,err := t.createCompositeKey(stub, t.prefixAddKey(objectTypePrefix, HistoryCompositeKey), attributes)
		historyBytes,err := t.getData(stub, collection, key); if err !=nil {
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

func (t *DbManager) getCompositeList(stub shim.ChaincodeStubInterface, objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool, op CompositeOp) ([]string,[][]byte,error) {
	var compositeKeys []string
	var compositeValues [][]byte
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return nil,nil,err
	}
	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	resultsIterator,err := t.getDataByPartialCompositeKey(stub, collection, objectType, prefixKeys)
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
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return nil,nil,err
		}
		_,compositeKeyParts, err := t.splitCompositeKey(stub, responseRange.Key)
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

		if op == CompositeKey || op == CompositeAll {
			index := len(keys) + len(prefixKeys)
			if index >= len(compositeKeyParts) {
				index = len(compositeKeyParts)
			}
			compositeKeys = append(compositeKeys, compositeKeyParts[index])
		}else if op == CompositeValue || op == CompositeAll {
			if filterVersion {
				compositeValues = append(compositeValues, responseRange.Value)
			}else {
				value,err := t.getDataByVersionBytes(stub, collection, objectTypePrefix, compositeKeyParts, responseRange.Value); if err !=nil {
					return nil,nil,err
				}
				compositeValues = append(compositeValues, value)
			}
		}
	}
	return compositeKeys,compositeValues,nil
}

////// State Operation //////
func (t *DbManager) putOrDelKey(stub shim.ChaincodeStubInterface, key string, value []byte, op Op) error {
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return err
	}
	if op == Set {
		return t.putData(stub, collection, key, value)
	}else if op == Del {
		return t.delData(stub, collection, key)
	}
	return nil
}

func (t *DbManager) getKey(stub shim.ChaincodeStubInterface, key string) ([]byte,error) {
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return nil,err
	}
	return t.getData(stub, collection, key)
}

func (t *DbManager) putOrDelData(stub shim.ChaincodeStubInterface, prefix string, key string, value []byte, op Op) error {
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return err
	}
	version,err := t.putDataHistory(stub, collection, prefix, []string{ key }, value, op); if err != nil {
		return err
	}

	return t.putVersion(stub, collection, t.prefixAddKey(prefix, key), version)
}

func (t *DbManager) putOrDelCompositeKeyData(stub shim.ChaincodeStubInterface, objectTypePrefix string, objectType string, attributes []string, value []byte, op Op) error {
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return err
	}
	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	key, err := t.createCompositeKey(stub, objectType, attributes)
	if err != nil {
		return err
	}
	version,err := t.putDataHistory(stub, collection, objectTypePrefix, attributes, value, op); if err != nil {
		return err
	}

	return t.putVersion(stub, collection, key, version)
}

func (t *DbManager) getCompositeKeyDataList(stub shim.ChaincodeStubInterface, objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32, filterVersion bool) ([][]byte,error) {
	_,compositeValues,err :=  t.getCompositeList(stub, objectTypePrefix, objectType, prefixKeys, keys, pageSize, filterVersion, CompositeValue); if err != nil {
		return nil,err
	}
	return compositeValues,nil
}

func (t *DbManager) getCompositeKeyList(stub shim.ChaincodeStubInterface, objectTypePrefix string, objectType string, prefixKeys []string, keys []string, pageSize int32) ([]string,error) {
	compositeKeys,_,err := t.getCompositeList(stub, objectTypePrefix, objectType, prefixKeys, keys, pageSize,false, CompositeKey); if err != nil {
		return nil,err
	}
	return compositeKeys,nil
}

func (t *DbManager) getCompositeKeyData(stub shim.ChaincodeStubInterface, objectTypePrefix string, objectType string, keys []string, filterVersion bool) ([]byte,error) {
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return nil,err
	}

	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	partialCompositeKey, err := t.createCompositeKey(stub, objectType, keys); if err != nil {
		return nil,err
	}
	versionBytes,err := t.getData(stub, collection, partialCompositeKey); if err !=nil {
		return nil,err
	}
	if filterVersion {
		return versionBytes,nil
	}else{
		return t.getDataByVersionBytes(stub, collection, objectTypePrefix, keys, versionBytes)
	}
}

func (t *DbManager) getCompositeKeyDataByVersion(stub shim.ChaincodeStubInterface, objectTypePrefix string, objectType string, keys []string, versionBytes []byte) ([]byte,error) {
	collection,err := t.getCollectionKey(stub)
	if err != nil {
		return nil,err
	}

	objectType = t.prefixAddKey(objectTypePrefix, objectType)
	return t.getDataByVersionBytes(stub, collection, objectTypePrefix, keys, versionBytes)
}

func (t *DbManager) getDataHistory(stub shim.ChaincodeStubInterface, objectTypePrefix string, keys []string, pageSize int32) ([][]byte,error) {
	return t.getCompositeKeyDataList(stub, objectTypePrefix, HistoryCompositeKey, keys, []string{}, pageSize,true)
}

/////////////////// Other State Function ///////////////////

func (t *DbManager) GetState(stub shim.ChaincodeStubInterface, collection,  key string) ([]byte,error) {
	return t.getData(stub, collection, key)
}

func (t *DbManager) GetStateByRange(stub shim.ChaincodeStubInterface, collection string,  startKey string, endKey string) ([]byte,error) {
	resultsIterator, err := t.getDataByRange(stub, collection, startKey, endKey)
	if err != nil {
		return nil,err
	}
	return t.getStateQueryIterator(resultsIterator)
}

func (t *DbManager) GetStateByPartialCompositeKey(stub shim.ChaincodeStubInterface, collection string,  objectType string, keys []string) ([]byte,error) {
	resultsIterator, err := t.getDataByPartialCompositeKey(stub, collection, objectType, keys)
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