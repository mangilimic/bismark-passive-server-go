package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"io"
	"log"
)

func AvailabilityPipeline(tracesStore transformer.StoreReader, intervalsStore, consolidatedStore, nodesStore transformer.Datastore, jsonWriter io.Writer, excludeRangesStore transformer.StoreWriter, timestamp int64, workers int) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "AvailabilityIntervals",
			Transformer: transformer.TransformFunc(AvailabilityIntervals),
			Reader:      tracesStore,
			Writer:      intervalsStore,
		},
		transformer.PipelineStage{
			Name:        "ConsolidateAvailabilityIntervals",
			Transformer: transformer.TransformFunc(ConsolidateAvailabilityIntervals),
			Reader:      intervalsStore,
			Writer:      consolidatedStore,
		},
		transformer.PipelineStage{
			Name:        "AvailabilityReducer",
			Transformer: transformer.TransformFunc(AvailabilityReducer),
			Reader:      consolidatedStore,
			Writer:      nodesStore,
		},
		transformer.PipelineStage{
			Name:   "AvailabilityJson",
			Reader: nodesStore,
			Writer: AvailabilityJsonStore{writer: jsonWriter, timestamp: timestamp},
		},
		transformer.PipelineStage{
			Name:        "GenerateExcludedRanges",
			Transformer: transformer.MakeMapFunc(GenerateExcludedRanges, workers),
			Reader:      consolidatedStore,
			Writer:      excludeRangesStore,
		},
	}
}

type IntervalKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
	FirstSequenceNumber  int32
	LastSequenceNumber   int32
}

func DecodeIntervalKey(encodedKey []byte) *IntervalKey {
	decodedKey := new(IntervalKey)
	key.DecodeOrDie(
		encodedKey,
		&decodedKey.NodeId,
		&decodedKey.AnonymizationContext,
		&decodedKey.SessionId,
		&decodedKey.FirstSequenceNumber,
		&decodedKey.LastSequenceNumber)
	return decodedKey
}

func EncodeIntervalKey(decodedKey *IntervalKey) []byte {
	return key.EncodeOrDie(
		decodedKey.NodeId,
		decodedKey.AnonymizationContext,
		decodedKey.SessionId,
		decodedKey.FirstSequenceNumber,
		decodedKey.LastSequenceNumber)
}

func AvailabilityIntervals(inputChan, outputChan chan *transformer.LevelDbRecord) {
	writeRecord := func(firstKey, lastKey *TraceKey, firstTrace, lastTrace []byte) {
		firstTraceDecoded := Trace{}
		if err := proto.Unmarshal(firstTrace, &firstTraceDecoded); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}
		lastTraceDecoded := Trace{}
		if err := proto.Unmarshal(lastTrace, &lastTraceDecoded); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}
		intervalKey := IntervalKey{
			NodeId:               firstKey.NodeId,
			AnonymizationContext: firstKey.AnonymizationContext,
			SessionId:            firstKey.SessionId,
			FirstSequenceNumber:  firstKey.SequenceNumber,
			LastSequenceNumber:   lastKey.SequenceNumber,
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   EncodeIntervalKey(&intervalKey),
			Value: key.EncodeOrDie(*firstTraceDecoded.TraceCreationTimestamp, *lastTraceDecoded.TraceCreationTimestamp),
		}
	}

	var firstKey, lastKey *TraceKey
	var firstTrace, lastTrace []byte
	var expectedTraceKey []byte
	for record := range inputChan {
		traceKey := DecodeTraceKey(record.Key)
		expectedNextTraceKey := EncodeTraceKey(&TraceKey{
			NodeId:               traceKey.NodeId,
			AnonymizationContext: traceKey.AnonymizationContext,
			SessionId:            traceKey.SessionId,
			SequenceNumber:       traceKey.SequenceNumber + 1,
		})

		if !bytes.Equal(expectedTraceKey, record.Key) {
			if firstTrace != nil {
				writeRecord(firstKey, lastKey, firstTrace, lastTrace)
			}
			firstKey = traceKey
			firstTrace = record.Value
		}
		lastKey = traceKey
		lastTrace = record.Value
		expectedTraceKey = expectedNextTraceKey
	}
	if firstTrace != nil {
		writeRecord(firstKey, lastKey, firstTrace, lastTrace)
	}
	close(outputChan)
}

func ConsolidateAvailabilityIntervals(inputChan, outputChan chan *transformer.LevelDbRecord) {
	writeRecord := func(firstKey, lastKey *IntervalKey, firstInterval, lastInterval []byte) {
		var firstIntervalStart, firstIntervalEnd, lastIntervalStart, lastIntervalEnd int64
		key.DecodeOrDie(firstInterval, &firstIntervalStart, &firstIntervalEnd)
		key.DecodeOrDie(lastInterval, &lastIntervalStart, &lastIntervalEnd)
		intervalKey := IntervalKey{
			NodeId:               firstKey.NodeId,
			AnonymizationContext: firstKey.AnonymizationContext,
			SessionId:            firstKey.SessionId,
			FirstSequenceNumber:  firstKey.FirstSequenceNumber,
			LastSequenceNumber:   lastKey.LastSequenceNumber,
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   EncodeIntervalKey(&intervalKey),
			Value: key.EncodeOrDie(firstIntervalStart, lastIntervalEnd),
		}
	}

	var firstIntervalKey, lastIntervalKey *IntervalKey
	var firstInterval, lastInterval []byte
	var previousSessionKeyEncoded []byte
	for record := range inputChan {
		intervalKey := DecodeIntervalKey(record.Key)
		sessionKey := &SessionKey{
			NodeId:               intervalKey.NodeId,
			AnonymizationContext: intervalKey.AnonymizationContext,
			SessionId:            intervalKey.SessionId,
		}
		sessionKeyEncoded := EncodeSessionKey(sessionKey)

		if !bytes.Equal(sessionKeyEncoded, previousSessionKeyEncoded) || intervalKey.FirstSequenceNumber != lastIntervalKey.LastSequenceNumber+1 {
			if firstIntervalKey != nil {
				writeRecord(firstIntervalKey, lastIntervalKey, firstInterval, lastInterval)
			}
			firstIntervalKey = intervalKey
			firstInterval = record.Value
		}
		previousSessionKeyEncoded = sessionKeyEncoded
		lastIntervalKey = intervalKey
		lastInterval = record.Value
	}
	if firstIntervalKey != nil {
		writeRecord(firstIntervalKey, lastIntervalKey, firstInterval, lastInterval)
	}
	close(outputChan)
}

func AvailabilityReducer(inputChan, outputChan chan *transformer.LevelDbRecord) {
	writeRecord := func(currentNode []byte, availability [][]int64) {
		if currentNode != nil {
			value, err := json.Marshal(availability)
			if err != nil {
				log.Fatalf("Error marshaling JSON: %v", err)
			}
			outputChan <- &transformer.LevelDbRecord{
				Key:   key.EncodeOrDie(currentNode),
				Value: value,
			}
		}
	}

	var currentNode []byte
	availability := make([][]int64, 2)
	for record := range inputChan {
		intervalKey := DecodeIntervalKey(record.Key)

		if !bytes.Equal(currentNode, intervalKey.NodeId) {
			writeRecord(currentNode, availability)
			currentNode = intervalKey.NodeId
			availability = make([][]int64, 2)
		}

		if intervalKey.FirstSequenceNumber == 0 {
			var startTimestamp, endTimestamp int64
			key.DecodeOrDie(record.Value, &startTimestamp, &endTimestamp)
			availability[0] = append(availability[0], startTimestamp*1000)
			availability[1] = append(availability[1], endTimestamp*1000)
		}
	}
	if currentNode != nil {
		writeRecord(currentNode, availability)
	}
	close(outputChan)
}

type AvailabilityJsonStore struct {
	writer    io.Writer
	timestamp int64
}

func (store AvailabilityJsonStore) Write(records chan *transformer.LevelDbRecord) error {
	if _, err := fmt.Fprintf(store.writer, "[{"); err != nil {
		return err
	}
	first := true
	for record := range records {
		var nodeId string
		key.DecodeOrDie(record.Key, &nodeId)
		if first {
			first = false
		} else {
			if _, err := fmt.Fprintf(store.writer, ","); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(store.writer, "\"%s\": %s", nodeId, record.Value); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(store.writer, "}, %d]", store.timestamp*1000); err != nil {
		return err
	}
	return nil
}

func GenerateExcludedRanges(record *transformer.LevelDbRecord) *transformer.LevelDbRecord {
	intervalKey := DecodeIntervalKey(record.Key)
	newKey := TraceKey{
		NodeId:               intervalKey.NodeId,
		AnonymizationContext: intervalKey.AnonymizationContext,
		SessionId:            intervalKey.SessionId,
		SequenceNumber:       intervalKey.FirstSequenceNumber,
	}
	newValue := TraceKey{
		NodeId:               intervalKey.NodeId,
		AnonymizationContext: intervalKey.AnonymizationContext,
		SessionId:            intervalKey.SessionId,
		SequenceNumber:       intervalKey.LastSequenceNumber,
	}
	return &transformer.LevelDbRecord{
		Key:   EncodeTraceKey(&newKey),
		Value: EncodeTraceKey(&newValue),
	}
}
