package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"log"
	"time"
)

func BytesPerMinuteMapper(inputChan, outputChan chan *LevelDbRecord) {
	nonce := int64(0)
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 5 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}

		trace := Trace{}
		if err := proto.Unmarshal(record.Value, &trace); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}

		buckets := make(map[int64]int64)
		for _, packetSeriesEntry := range trace.PacketSeries {
			timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds*1000)
			minuteTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), 0, 0, time.UTC)
			buckets[minuteTimestamp.Unix()] += int64(*packetSeriesEntry.Size)
		}

		for timestamp, size := range buckets {
			timestampBytes, err := encodeLexicographicInt64(timestamp)
			if err != nil {
				log.Printf("Error encoding timestamp: %v", err)
				continue
			}
			nonceBytes, err := encodeLexicographicInt64(nonce)
			if err != nil {
				log.Fatalf("Error encoding nonce: %v", err)
			}
			nonce++
			outputChan <- &LevelDbRecord{
				Key:   makeKey("bytes_per_minute_mapped", key[1], timestampBytes, nonceBytes),
				Value: encodeInt64(size),
			}
		}
	}
}

func BytesPerMinuteReducer(inputChan, outputChan chan *LevelDbRecord) {
	var currentSize int64
	var currentNode, currentTimestamp []byte
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 4 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}
		if currentNode == nil || currentTimestamp == nil {
			currentNode = key[1]
			currentTimestamp = key[2]
		}
		if !bytes.Equal(key[1], currentNode) || !bytes.Equal(key[2], currentTimestamp) {
			outputChan <- &LevelDbRecord{
				Key:   makeKey("bytes_per_minute", currentNode, currentTimestamp),
				Value: encodeInt64(currentSize),
			}
			currentNode = key[1]
			currentTimestamp = key[2]
			currentSize = 0
		}

		value, err := decodeInt64(record.Value)
		if err != nil {
			log.Fatal("Error decoding size from VarInt: %v", err)
		}
		currentSize += value
	}
	if currentNode != nil && currentTimestamp != nil {
		outputChan <- &LevelDbRecord{
			Key:   makeKey("bytes_per_minute", currentNode, currentTimestamp),
			Value: encodeInt64(currentSize),
		}
	}
}
