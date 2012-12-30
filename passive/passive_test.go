package passive_test

import (
	. "bismark/passive"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"fmt"
	"sort"
)

func encodeProto(message proto.Message) (encoded []byte) {
	encoded, err := proto.Marshal(message)
	if err != nil {
		panic(fmt.Sprintf("Error marshaling protocol buffer: %v", err))
	}
	return
}

func decodeProto(encoded []byte, decoded proto.Message) {
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		panic(fmt.Sprintf("Error unmarshaling protocol buffer: %v", err))
	}
}

func decodeInt64(encoded []byte) int64 {
	value, bytesRead := binary.Varint(encoded)
	if value == 0 && bytesRead <= 0 {
		panic("Error decoding VarInt")
	}
	return value
}

func createLevelDbRecord(key string, value proto.Message) *LevelDbRecord {
	return &LevelDbRecord{
		Key: []byte(key),
		Value: encodeProto(value),
	}
}

func runTransform(transform func(inputChan, outputChan chan *LevelDbRecord), inputRecords []*LevelDbRecord, inputTable string) []*LevelDbRecord {
	inputChan := make(chan *LevelDbRecord, 100)
	outputChan := make(chan *LevelDbRecord, 100)
	for _, record := range inputRecords {
		if !bytes.HasPrefix(record.Key, bytes.Join([][]byte{[]byte(inputTable), []byte(":")}, []byte{})) {
			continue
		}
		inputChan <- record
	}
	close(inputChan)
	transform(inputChan, outputChan)
	close(outputChan)
	outputRecords := []*LevelDbRecord{}
	for record := range outputChan {
		outputRecords = append(outputRecords, record)
	}
	sort.Sort(LevelDbRecordSlice(outputRecords))
	return outputRecords
}

func mergeOutputs(first, second []*LevelDbRecord) []*LevelDbRecord {
	merged := []*LevelDbRecord{}
	for _, record := range first {
		merged = append(merged, record)
	}
	for _, record := range second {
		merged = append(merged, record)
	}
	sort.Sort(LevelDbRecordSlice(merged))
	return merged
}
