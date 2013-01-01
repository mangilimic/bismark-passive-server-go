package passive

import (
	"fmt"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"time"
	"log"
	"io"
	"encoding/json"
)

func AvailabilityMapper(inputChan, outputChan chan *LevelDbRecord) {
	var nonce Nonce
	var currentNode []byte
	var expectedSequenceNumber int32
	var currentBeginning []byte
	var currentSession []byte
	var currentValue []byte
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 5 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}

		session := bytes.Join(key[1:4], []byte(":"))
		if currentSession == nil {
			currentSession = session
		}
		if currentNode == nil {
			currentNode = key[1]
		}

		if !bytes.Equal(currentSession, session) {
			if currentValue != nil {
				trace := Trace{}
				if err := proto.Unmarshal(currentValue, &trace); err != nil {
					log.Fatalf("Error ummarshaling protocol buffer: %v", err)
				}
				if trace.TraceCreationTimestamp != nil {
					currentEnding, err := encodeLexicographicInt64(*trace.TraceCreationTimestamp)
					if err != nil {
						log.Fatalf("Error encoding interval ending timestamp %q: %v", *trace.TraceCreationTimestamp, err)
					}
					outputChan <- &LevelDbRecord{
						Key: makeKey("availability_with_nonce", currentNode, nonce.Next()),
						Value: bytes.Join([][]byte{currentBeginning, currentEnding}, []byte(",")),
					}
				}
			}
			currentNode = key[1]
			currentSession = session
			currentValue = nil
			expectedSequenceNumber = 0
		}

		sequenceNumber, err := decodeLexicographicInt32(key[4])
		if err != nil {
			log.Fatalf("Error decoding sequence number: %v", err)
		}
		if sequenceNumber != expectedSequenceNumber {
			continue
		}
		if sequenceNumber == 0 {
			trace := Trace{}
			if err := proto.Unmarshal(record.Value, &trace); err != nil {
				log.Fatalf("Error ummarshaling protocol buffer: %v", err)
			}
			if trace.TraceCreationTimestamp == nil {
				continue
			}
			currentBeginning, err = encodeLexicographicInt64(*trace.TraceCreationTimestamp)
			if err != nil {
				log.Fatalf("Error encoding interval start timestamp %q: %v", *trace.TraceCreationTimestamp, err)
			}
		}
		expectedSequenceNumber++
		currentValue = record.Value
	}
	if currentValue != nil {
		trace := Trace{}
		if err := proto.Unmarshal(currentValue, &trace); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}
		if trace.TraceCreationTimestamp != nil {
			currentEnding, err := encodeLexicographicInt64(*trace.TraceCreationTimestamp)
			if err != nil {
				log.Fatalf("Error encoding interval ending timestamp %q: %v", *trace.TraceCreationTimestamp, err)
			}
			outputChan <- &LevelDbRecord{
				Key: makeKey("availability_with_nonce", currentNode, nonce.Next()),
				Value: bytes.Join([][]byte{currentBeginning, currentEnding}, []byte(",")),
			}
		}
	}
}

func AvailabilityReducer(inputChan, outputChan chan *LevelDbRecord) {
	var currentNode []byte
	availability := make([][]int64, 2)
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 3 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}
		nodeId := key[1]
		if currentNode == nil {
			currentNode = nodeId
		}

		if !bytes.Equal(currentNode, nodeId) {
			value, err := json.Marshal(availability)
			if err != nil {
				log.Fatalf("Error marshaling JSON: %v", err)
			}
			outputChan <- &LevelDbRecord{
				Key: makeKey("availability", currentNode),
				Value: value,
			}
			currentNode = nodeId
			availability = make([][]int64, 2)
		}

		startAndEnd := bytes.Split(record.Value, []byte(","))
		startTimestamp, err := decodeLexicographicInt64(startAndEnd[0])
		if err != nil {
			log.Fatalf("Error decoding start timestamp")
		}
		endTimestamp, err := decodeLexicographicInt64(startAndEnd[1])

		availability[0] = append(availability[0], startTimestamp * 1000)
		availability[1] = append(availability[1], endTimestamp * 1000)
	}

	if currentNode != nil {
		value, err := json.Marshal(availability)
		if err != nil {
			log.Fatalf("Error marshaling JSON: %v", err)
		}
		outputChan <- &LevelDbRecord{
			Key: makeKey("availability", currentNode),
			Value: value,
		}
	}
}

type AvailabilityJson struct {
	io.Writer
}

func (writer AvailabilityJson) Do(inputChan, outputChan chan *LevelDbRecord) {
	fmt.Fprintf(writer, "[{")
	first := true
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 2 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}
		nodeId := key[1]
		if first {
			first = false
		} else {
			fmt.Fprintf(writer, ",")
		}
		fmt.Fprintf(writer, "\"%s\": %s", nodeId, record.Value)
	}
	fmt.Fprintf(writer, "}, %d]", time.Now().Unix() * 1000)
}
