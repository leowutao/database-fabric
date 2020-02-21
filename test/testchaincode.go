package test

import (
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	pb "github.com/hyperledger/fabric/protos/peer"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const  (
	minUnicodeRuneValue   = 0            //U+0000
	maxUnicodeRuneValue   = utf8.MaxRune //U+10FFFF - maximum (and unallocated) code point
	compositeKeyNamespace = "\x00"
	emptyKeySubstitute    = "\x01"
)

type TestChaincodeStub struct {
	Data map[string]map[string][]byte
	PutData map[string]map[string][]byte
	Args []string
	txIdNum int64

	TxID                       string
	ChannelId                  string
	chaincodeEvent             *pb.ChaincodeEvent
	signedProposal             *pb.SignedProposal
	proposal                   *pb.Proposal
	validationParameterMetakey string

	// Additional fields extracted from the signedProposal
	creator   []byte
	Transient map[string][]byte
	binding   []byte

	decorations map[string][]byte
}

func (stub *TestChaincodeStub) GetTxTimestamp() (*timestamp.Timestamp, error) {
	t := timestamp.Timestamp{}
	t.Seconds = int64(time.Now().Unix())
	return &t,nil
}

func (stub *TestChaincodeStub) GetTxID() string {
	if stub.TxID == "" {
		stub.txIdNum++
		return strconv.FormatInt(stub.txIdNum, 10)
	}
	return stub.TxID
}

func (stub *TestChaincodeStub) GetArgs()[][]byte {
	var args [][]byte
	for _,v := range stub.Args {
		args = append(args, []byte(v))
	}
	return args
}

func (stub *TestChaincodeStub) GetStringArgs()[]string {
	return stub.Args
}

func (stub *TestChaincodeStub) GetFunctionAndParameters() (function string, params []string) {
	allargs := stub.GetStringArgs()
	function = ""
	params = []string{}
	if len(allargs) >= 1 {
		function = allargs[0]
		params = allargs[1:]
	}
	return
}


func (stub *TestChaincodeStub) GetArgsSlice() ([]byte, error){
	return nil,nil
}

func (stub *TestChaincodeStub) GetChannelID() string {
	return stub.ChannelId
}

func (stub *TestChaincodeStub) InvokeChaincode(chaincodeName string, args [][]byte, channel string) pb.Response {
	return pb.Response{}
}

func (stub *TestChaincodeStub) SetStateValidationParameter(key string, ep []byte) error {
	return nil
}

func (stub *TestChaincodeStub) GetStateValidationParameter(key string) ([]byte, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) GetStateByRange(startKey, endKey string) (shim.StateQueryIteratorInterface, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) GetStateByRangeWithPagination(startKey, endKey string, pageSize int32,
	bookmark string) (shim.StateQueryIteratorInterface, *pb.QueryResponseMetadata, error) {
	return nil,nil,nil
}

func (stub *TestChaincodeStub) GetStateByPartialCompositeKeyWithPagination(objectType string, keys []string,
	pageSize int32, bookmark string) (shim.StateQueryIteratorInterface, *pb.QueryResponseMetadata, error) {
	return nil,nil,nil
}

func (stub *TestChaincodeStub) GetQueryResult(query string) (shim.StateQueryIteratorInterface, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) GetQueryResultWithPagination(query string, pageSize int32,
	bookmark string) (shim.StateQueryIteratorInterface, *pb.QueryResponseMetadata, error) {
	return nil,nil,nil
}

func (stub *TestChaincodeStub) GetHistoryForKey(key string) (shim.HistoryQueryIteratorInterface, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) SetPrivateDataValidationParameter(collection, key string, ep []byte) error {
	return nil
}

func (stub *TestChaincodeStub) GetPrivateDataValidationParameter(collection, key string) ([]byte, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) GetPrivateDataQueryResult(collection, query string) (shim.StateQueryIteratorInterface, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) GetCreator() ([]byte, error) {
	return stub.creator,nil
}

func (stub *TestChaincodeStub) GetTransient() (map[string][]byte, error) {
	return stub.Transient,nil
}

func (stub *TestChaincodeStub) GetBinding() ([]byte, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) GetDecorations() map[string][]byte {
	return nil
}

func (stub *TestChaincodeStub) GetSignedProposal() (*pb.SignedProposal, error) {
	return nil,nil
}

func (stub *TestChaincodeStub) SetEvent(name string, payload []byte) error {
	return nil
}

func (stub *TestChaincodeStub) GetPrivateDataHash(collection string, key string) ([]byte, error) {
	return nil,nil
}

var DEFAULT_COLLECTION = "COLLECTION"

func (stub *TestChaincodeStub) PutState(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key must not be an empty string")
	}
	if stub.PutData == nil {
		stub.PutData = map[string]map[string][]byte{}
	}
	if stub.PutData[DEFAULT_COLLECTION] == nil {
		stub.PutData[DEFAULT_COLLECTION] = map[string][]byte{}
	}
	stub.PutData[DEFAULT_COLLECTION][key] = value
	return nil
}

func (stub *TestChaincodeStub) DelState(key string) error {
	if key == "" {
		return fmt.Errorf("key must not be an empty string")
	}
	if stub.Data == nil {
		stub.Data = map[string]map[string][]byte{}
	}
	if stub.Data[DEFAULT_COLLECTION] == nil {
		stub.Data[DEFAULT_COLLECTION] = map[string][]byte{}
	}
	delete(stub.Data[DEFAULT_COLLECTION], key)
	return nil
}

func (stub *TestChaincodeStub) GetState(key string) ([]byte, error) {
	if key == "" {
		return nil,fmt.Errorf("key must not be an empty string")
	}
	var value []byte
	if stub.Data[DEFAULT_COLLECTION] != nil {
		value = stub.Data[DEFAULT_COLLECTION][key]
	}
	return value,nil
}


func (stub *TestChaincodeStub) GetStateByPartialCompositeKey(objectType string, keys []string) (shim.StateQueryIteratorInterface, error) {
	startKey, endKey, err := stub.createRangeKeysForPartialCompositeKey(objectType, keys)
	if err != nil {
		return nil, err
	}

	return stub.handleGetStateByRange(DEFAULT_COLLECTION, startKey, endKey)
}

// CommonIterator documentation can be found in interfaces.go
type CommonIterator struct {
	channelId  string
	txid       string
	currentLoc int
	response []map[string][]byte
}

// StateQueryIterator documentation can be found in interfaces.go
type StateQueryIterator struct {
	*CommonIterator
}

// HasNext documentation can be found in interfaces.go
func (iter *CommonIterator) HasNext() bool {
	if iter.currentLoc < len(iter.response) {
		return true
	}
	return false
}

// Close documentation can be found in interfaces.go
func (iter *CommonIterator) Close() error {
	return nil
}

func (iter *StateQueryIterator) Next() (*queryresult.KV, error) {
	if iter.currentLoc < len(iter.response) {
		result := iter.response[iter.currentLoc]
		iter.currentLoc++
		for k,v := range result {
			return &queryresult.KV{"", k,v, struct{}{},nil,0 },nil
		}
	}
	return nil, fmt.Errorf("result not found")

}

//  ---------  private state functions  ---------

// GetPrivateData documentation can be found in interfaces.go
func (stub *TestChaincodeStub) GetPrivateData(collection string, key string) ([]byte, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection must not be an empty string")
	}
	var value []byte
	if stub.Data[collection] != nil {
		value = stub.Data[collection][key]
	}
	return value,nil
}

// PutPrivateData documentation can be found in interfaces.go
func (stub *TestChaincodeStub) PutPrivateData(collection string, key string, value []byte) error {
	if collection == "" {
		return fmt.Errorf("collection must not be an empty string")
	}
	if key == "" {
		return fmt.Errorf("key must not be an empty string")
	}
	if stub.PutData == nil {
		stub.PutData = map[string]map[string][]byte{}
	}
	if stub.PutData[collection] == nil {
		stub.PutData[collection] = map[string][]byte{}
	}
	stub.PutData[collection][key] = value
	return nil
}

// DelPrivateData documentation can be found in interfaces.go
func (stub *TestChaincodeStub) DelPrivateData(collection string, key string) error {
	if collection == "" {
		return fmt.Errorf("collection must not be an empty string")
	}
	return nil
}

//CreateCompositeKey documentation can be found in interfaces.go
func (stub *TestChaincodeStub) CreateCompositeKey(objectType string, attributes []string) (string, error) {
	return createCompositeKey(objectType, attributes)
}


func (stub *TestChaincodeStub) SplitCompositeKey(compositeKey string) (string, []string, error) {
	return splitCompositeKey(compositeKey)
}

func createCompositeKey(objectType string, attributes []string) (string, error) {
	if err := validateCompositeKeyAttribute(objectType); err != nil {
		return "", err
	}
	ck := compositeKeyNamespace + objectType + string(minUnicodeRuneValue)
	for _, att := range attributes {
		if err := validateCompositeKeyAttribute(att); err != nil {
			return "", err
		}
		ck += att + string(minUnicodeRuneValue)
	}
	return ck, nil
}

func splitCompositeKey(compositeKey string) (string, []string, error) {
	componentIndex := 1
	components := []string{}
	for i := 1; i < len(compositeKey); i++ {
		if compositeKey[i] == minUnicodeRuneValue {
			components = append(components, compositeKey[componentIndex:i])
			componentIndex = i + 1
		}
	}
	return components[0], components[1:], nil
}

func validateCompositeKeyAttribute(str string) error {
	if !utf8.ValidString(str) {
		return fmt.Errorf("not a valid utf8 string: [%x]", str)
	}
	for index, runeValue := range str {
		if runeValue == minUnicodeRuneValue || runeValue == maxUnicodeRuneValue {
			return fmt.Errorf(`input contain unicode %#U starting at position [%d]. %#U and %#U are not allowed in the input attribute of a composite key`,
				runeValue, index, minUnicodeRuneValue, maxUnicodeRuneValue)
		}
	}
	return nil
}

func validateSimpleKeys(simpleKeys ...string) error {
	for _, key := range simpleKeys {
		if len(key) > 0 && key[0] == compositeKeyNamespace[0] {
			return fmt.Errorf(`first character of the key [%s] contains a null character which is not allowed`, key)
		}
	}
	return nil
}

func (stub *TestChaincodeStub) createRangeKeysForPartialCompositeKey(objectType string, attributes []string) (string, string, error) {
	partialCompositeKey, err := stub.CreateCompositeKey(objectType, attributes)
	if err != nil {
		return "", "", err
	}
	startKey := partialCompositeKey
	endKey := partialCompositeKey + string(maxUnicodeRuneValue)

	return startKey, endKey, nil
}

// GetPrivateDataByPartialCompositeKey documentation can be found in interfaces.go
func (stub *TestChaincodeStub) GetPrivateDataByPartialCompositeKey(collection, objectType string, attributes []string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection must not be an empty string")
	}

	startKey, endKey, err := stub.createRangeKeysForPartialCompositeKey(objectType, attributes)
	if err != nil {
		return nil, err
	}

	return stub.handleGetStateByRange(collection, startKey, endKey)
}

func (stub *TestChaincodeStub) GetPrivateDataByRange(collection, startKey, endKey string) (shim.StateQueryIteratorInterface, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection must not be an empty string")
	}
	if startKey == "" {
		startKey = emptyKeySubstitute
	}
	if err := validateSimpleKeys(startKey, endKey); err != nil {
		return nil, err
	}

	return stub.handleGetStateByRange(collection, startKey, endKey)
}

func (stub *TestChaincodeStub) handleGetStateByRange(collection, startKey, endKey string) (shim.StateQueryIteratorInterface, error) {
	var response []map[string][]byte
	data := stub.Data[collection]
	keys := FilterKey(endKey, data)
	keys = OrderByKey(keys)

	first := false
	for _,k := range keys {

		if !first && strings.Index(k,startKey) < 0 {
			continue
		}
		first = true

		if strings.Index(endKey, string(maxUnicodeRuneValue)) > 0 {
			newEndKey := strings.Replace(endKey, string(maxUnicodeRuneValue), "", 1)
			if strings.Index(k,newEndKey) < 0  {
				break
			}
		}

		response = append(response, map[string][]byte{k:data[k]})

		if strings.Index(k,endKey) >= 0 {
			break
		}
	}

	iterator := createStateQueryIterator(response)
	return iterator,nil
}

func FilterKey(endKey string, data map[string][]byte) []string {
	var keys []string
	splitKeys := strings.Split(endKey, string(compositeKeyNamespace))
	key := ""
	for i,k := range splitKeys {
		if i < len(splitKeys) - 1 {
			key += k + string(compositeKeyNamespace)
		}
	}
	for k,_ := range data {
		if strings.Index(k, key) >= 0 {
			keys = append(keys, k)
		}
	}

	return keys
}

type Keys []string
func (p Keys) Len() int           { return len(p) }
func (p Keys) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
type SortByLength struct{ Keys }
func (p SortByLength) Less(i, j int) bool {	return len(p.Keys[i]) < len(p.Keys[j]) }
type SortByName struct{ Keys }
func (p SortByName) Less(i, j int) bool { return p.Keys[i] < p.Keys[j] && len(p.Keys[i]) == len(p.Keys[j]) }

func OrderByKey(keys []string) []string {
	sort.Sort(SortByLength{keys})

	//str,_ := t.ConvertJsonBytes(keys)
	//fmt.Println(string(str))

	sort.Sort(SortByName{keys})

	//str1,_ := t.ConvertJsonBytes(keys)
	//fmt.Println(string(str1))

	return keys
}

func createStateQueryIterator(response []map[string][]byte) *StateQueryIterator {
	return &StateQueryIterator{CommonIterator: &CommonIterator{
		channelId:  "",
		txid:       "",
		response:   response,
		currentLoc: 0}}
}

func (stub *TestChaincodeStub) MergePutData(){
	if stub.PutData != nil && len(stub.PutData) > 0 {
		if stub.Data == nil {
			stub.Data =  map[string]map[string][]byte{}
		}
		for k,v := range stub.PutData {
			stub.Data[k] = v
		}
	}
}