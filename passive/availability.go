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

func AvailabilityPipeline(tracesStore transformer.StoreSeeker, intervalsStore transformer.Datastore, consolidatedStore, nodesStore transformer.DatastoreFull, jsonWriter io.Writer, excludeRangesStore transformer.DatastoreFull, consistentRangesStore transformer.StoreDeleter, timestamp int64, workers int) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "AvailabilityIntervals",
			Reader:      transformer.ReadExcludingRanges(tracesStore, excludeRangesStore),
			Transformer: transformer.TransformFunc(AvailabilityIntervals),
			Writer:      intervalsStore,
		},
		transformer.PipelineStage{
			Name:        "ConsolidateAvailabilityIntervals",
			Reader:      intervalsStore,
			Transformer: transformer.TransformFunc(ConsolidateAvailabilityIntervals),
			Writer:      transformer.TruncateBeforeWriting(consolidatedStore),
		},
		transformer.PipelineStage{
			Name:        "AvailabilityReducer",
			Reader:      consolidatedStore,
			Transformer: transformer.TransformFunc(AvailabilityReducer),
			Writer:      transformer.TruncateBeforeWriting(nodesStore),
		},
		transformer.PipelineStage{
			Name:   "AvailabilityJson",
			Reader: nodesStore,
			Writer: &AvailabilityJsonStore{writer: jsonWriter, timestamp: timestamp},
		},
		transformer.PipelineStage{
			Name:        "GenerateExcludedRanges",
			Reader:      consolidatedStore,
			Transformer: transformer.MakeMapFunc(GenerateExcludedRanges, workers),
			Writer:      transformer.TruncateBeforeWriting(excludeRangesStore),
		},
		transformer.PipelineStage{
			Name:        "GenerateConsistentRanges",
			Reader:      excludeRangesStore,
			Transformer: transformer.MakeDoFunc(GenerateConsistentRanges, workers),
			Writer:      transformer.TruncateBeforeWriting(consistentRangesStore),
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
		var traceKey TraceKey
		key.DecodeOrDie(record.Key, &traceKey)
		expectedNextTraceKeyDecoded := TraceKey{
			NodeId:               traceKey.NodeId,
			AnonymizationContext: traceKey.AnonymizationContext,
			SessionId:            traceKey.SessionId,
			SequenceNumber:       traceKey.SequenceNumber + 1,
		}
		expectedNextTraceKey := key.EncodeOrDie(&expectedNextTraceKeyDecoded)

		if !bytes.Equal(expectedTraceKey, record.Key) {
			if firstTrace != nil {
				writeRecord(firstKey, lastKey, firstTrace, lastTrace)
			}
			firstKey = &traceKey
			firstTrace = record.Value
		}
		lastKey = &traceKey
		lastTrace = record.Value
		expectedTraceKey = expectedNextTraceKey
	}
	if firstTrace != nil {
		writeRecord(firstKey, lastKey, firstTrace, lastTrace)
	}
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
		sessionKey := SessionKey{
			NodeId:               intervalKey.NodeId,
			AnonymizationContext: intervalKey.AnonymizationContext,
			SessionId:            intervalKey.SessionId,
		}
		sessionKeyEncoded := key.EncodeOrDie(&sessionKey)

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
}

func AvailabilityReducer(inputChan, outputChan chan *transformer.LevelDbRecord) {
	writeRecord := func(currentNode []byte, points [][]int64) {
		if currentNode != nil {
			value, err := json.Marshal(points)
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
	points := make([][]int64, 4)
	for record := range inputChan {
		intervalKey := DecodeIntervalKey(record.Key)

		if !bytes.Equal(currentNode, intervalKey.NodeId) {
			writeRecord(currentNode, points)
			currentNode = intervalKey.NodeId
			points = make([][]int64, 4)
		}

		var startTimestamp, endTimestamp int64
		key.DecodeOrDie(record.Value, &startTimestamp, &endTimestamp)
		if intervalKey.FirstSequenceNumber == 0 {
			points[0] = append(points[0], startTimestamp*1000)
			points[1] = append(points[1], endTimestamp*1000)
		} else {
			points[2] = append(points[2], startTimestamp*1000)
			points[3] = append(points[3], endTimestamp*1000)
		}
	}
	if currentNode != nil {
		writeRecord(currentNode, points)
	}
}

type AvailabilityJsonStore struct {
	writer    io.Writer
	timestamp int64
	first     bool
}

func (store *AvailabilityJsonStore) BeginWriting() error {
	if _, err := fmt.Fprintf(store.writer, "[{"); err != nil {
		return err
	}
	store.first = true
	return nil
}

func (store *AvailabilityJsonStore) WriteRecord(record *transformer.LevelDbRecord) error {
	var nodeId string
	key.DecodeOrDie(record.Key, &nodeId)
	if store.first {
		store.first = false
	} else {
		if _, err := fmt.Fprintf(store.writer, ","); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(store.writer, "\"%s\": %s", nodeId, record.Value); err != nil {
		return err
	}
	return nil
}

func (store *AvailabilityJsonStore) EndWriting() error {
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
		Key:   key.EncodeOrDie(&newKey),
		Value: key.EncodeOrDie(&newValue),
	}
}

func GenerateConsistentRanges(record *transformer.LevelDbRecord, outputChan chan *transformer.LevelDbRecord) {
	var intervalStartKey TraceKey
	key.DecodeOrDie(record.Key, &intervalStartKey)
	if intervalStartKey.SequenceNumber != 0 {
		return
	}
	outputChan <- record
}
