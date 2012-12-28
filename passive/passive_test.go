package passive_test

import (
	. "bismark/passive"
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

func sortChan(inputChan chan *LevelDbRecord) (chan *LevelDbRecord) {
	outputChan := make(chan *LevelDbRecord, 100)
	records := make([]*LevelDbRecord, 0)
	for record := range inputChan {
		records = append(records, record)
	}
	sort.Sort(LevelDbRecordSlice(records))
	for _, record := range records {
		outputChan <- record
	}
	close(outputChan)
	return outputChan
}

