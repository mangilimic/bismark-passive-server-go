package passive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func AvailabilityPipeline(levelDbManager store.Manager, jsonWriter io.Writer, timestamp int64, workers int) transformer.Pipeline {
	tracesStore := levelDbManager.Seeker("traces")
	intervalsStore := levelDbManager.ReadingWriter("availablity-intervals")
	consolidatedStore := levelDbManager.ReadingDeleter("availability-consolidated")
	nodesStore := levelDbManager.ReadingDeleter("availability-nodes")
	excludeRangesStore := levelDbManager.ReadingDeleter("availability-done")
	consistentRangesStore := levelDbManager.ReadingDeleter("consistent-ranges")
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "AvailabilityIntervals",
			Reader:      store.NewRangeExcludingReader(tracesStore, excludeRangesStore),
			Transformer: transformer.TransformFunc(availabilityIntervals),
			Writer:      intervalsStore,
		},
		transformer.PipelineStage{
			Name:        "ConsolidateAvailabilityIntervals",
			Reader:      intervalsStore,
			Transformer: transformer.TransformFunc(consolidateAvailabilityIntervals),
			Writer:      store.NewTruncatingWriter(consolidatedStore),
		},
		transformer.PipelineStage{
			Name:        "AvailabilityReducer",
			Reader:      consolidatedStore,
			Transformer: transformer.TransformFunc(availabilityReducer),
			Writer:      store.NewTruncatingWriter(nodesStore),
		},
		transformer.PipelineStage{
			Name:   "AvailabilityJson",
			Reader: nodesStore,
			Writer: &availabilityJsonStore{writer: jsonWriter, timestamp: timestamp},
		},
		transformer.PipelineStage{
			Name:        "GenerateExcludedRanges",
			Reader:      consolidatedStore,
			Transformer: transformer.MakeMapFunc(generateExcludedRanges, workers),
			Writer:      store.NewTruncatingWriter(excludeRangesStore),
		},
		transformer.PipelineStage{
			Name:        "GenerateConsistentRanges",
			Reader:      excludeRangesStore,
			Transformer: transformer.MakeDoFunc(generateConsistentRanges, workers),
			Writer:      store.NewTruncatingWriter(consistentRangesStore),
		},
	}
}

type intervalKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
	FirstSequenceNumber  int32
	LastSequenceNumber   int32
}

func decodeIntervalKey(encodedKey []byte) *intervalKey {
	decodedKey := new(intervalKey)
	lex.DecodeOrDie(
		encodedKey,
		&decodedKey.NodeId,
		&decodedKey.AnonymizationContext,
		&decodedKey.SessionId,
		&decodedKey.FirstSequenceNumber,
		&decodedKey.LastSequenceNumber)
	return decodedKey
}

func encodeIntervalKey(decodedKey *intervalKey) []byte {
	return lex.EncodeOrDie(
		decodedKey.NodeId,
		decodedKey.AnonymizationContext,
		decodedKey.SessionId,
		decodedKey.FirstSequenceNumber,
		decodedKey.LastSequenceNumber)
}

func availabilityIntervals(inputChan, outputChan chan *store.Record) {
	writeRecord := func(firstKey, lastKey *TraceKey, firstTrace, lastTrace []byte) {
		firstTraceDecoded := Trace{}
		if err := proto.Unmarshal(firstTrace, &firstTraceDecoded); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}
		lastTraceDecoded := Trace{}
		if err := proto.Unmarshal(lastTrace, &lastTraceDecoded); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}
		intervalKey := intervalKey{
			NodeId:               firstKey.NodeId,
			AnonymizationContext: firstKey.AnonymizationContext,
			SessionId:            firstKey.SessionId,
			FirstSequenceNumber:  firstKey.SequenceNumber,
			LastSequenceNumber:   lastKey.SequenceNumber,
		}
		outputChan <- &store.Record{
			Key:   encodeIntervalKey(&intervalKey),
			Value: lex.EncodeOrDie(*firstTraceDecoded.TraceCreationTimestamp, *lastTraceDecoded.TraceCreationTimestamp),
		}
	}

	var firstKey, lastKey *TraceKey
	var firstTrace, lastTrace []byte
	var expectedTraceKey []byte
	for record := range inputChan {
		var traceKey TraceKey
		lex.DecodeOrDie(record.Key, &traceKey)
		expectedNextTraceKeyDecoded := TraceKey{
			NodeId:               traceKey.NodeId,
			AnonymizationContext: traceKey.AnonymizationContext,
			SessionId:            traceKey.SessionId,
			SequenceNumber:       traceKey.SequenceNumber + 1,
		}
		expectedNextTraceKey := lex.EncodeOrDie(&expectedNextTraceKeyDecoded)

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

func consolidateAvailabilityIntervals(inputChan, outputChan chan *store.Record) {
	writeRecord := func(firstKey, lastKey *intervalKey, firstInterval, lastInterval []byte) {
		var firstIntervalStart, firstIntervalEnd, lastIntervalStart, lastIntervalEnd int64
		lex.DecodeOrDie(firstInterval, &firstIntervalStart, &firstIntervalEnd)
		lex.DecodeOrDie(lastInterval, &lastIntervalStart, &lastIntervalEnd)
		intervalKey := intervalKey{
			NodeId:               firstKey.NodeId,
			AnonymizationContext: firstKey.AnonymizationContext,
			SessionId:            firstKey.SessionId,
			FirstSequenceNumber:  firstKey.FirstSequenceNumber,
			LastSequenceNumber:   lastKey.LastSequenceNumber,
		}
		outputChan <- &store.Record{
			Key:   encodeIntervalKey(&intervalKey),
			Value: lex.EncodeOrDie(firstIntervalStart, lastIntervalEnd),
		}
	}

	var firstIntervalKey, lastIntervalKey *intervalKey
	var firstInterval, lastInterval []byte
	var previousSessionKeyEncoded []byte
	for record := range inputChan {
		intervalKey := decodeIntervalKey(record.Key)
		sessionKey := SessionKey{
			NodeId:               intervalKey.NodeId,
			AnonymizationContext: intervalKey.AnonymizationContext,
			SessionId:            intervalKey.SessionId,
		}
		sessionKeyEncoded := lex.EncodeOrDie(&sessionKey)

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

func availabilityReducer(inputChan, outputChan chan *store.Record) {
	writeRecord := func(currentNode []byte, points [][]int64) {
		if currentNode != nil {
			value, err := json.Marshal(points)
			if err != nil {
				log.Fatalf("Error marshaling JSON: %v", err)
			}
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(currentNode),
				Value: value,
			}
		}
	}

	var currentNode []byte
	points := make([][]int64, 4)
	for record := range inputChan {
		intervalKey := decodeIntervalKey(record.Key)

		if !bytes.Equal(currentNode, intervalKey.NodeId) {
			writeRecord(currentNode, points)
			currentNode = intervalKey.NodeId
			points = make([][]int64, 4)
		}

		var startTimestamp, endTimestamp int64
		lex.DecodeOrDie(record.Value, &startTimestamp, &endTimestamp)
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

type availabilityJsonStore struct {
	writer    io.Writer
	timestamp int64
	first     bool
}

func (store *availabilityJsonStore) BeginWriting() error {
	if _, err := fmt.Fprintf(store.writer, "[{"); err != nil {
		return err
	}
	store.first = true
	return nil
}

func (store *availabilityJsonStore) WriteRecord(record *store.Record) error {
	var nodeId string
	lex.DecodeOrDie(record.Key, &nodeId)
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

func (store *availabilityJsonStore) EndWriting() error {
	if _, err := fmt.Fprintf(store.writer, "}, %d]", store.timestamp*1000); err != nil {
		return err
	}
	return nil
}

func generateExcludedRanges(record *store.Record) *store.Record {
	intervalKey := decodeIntervalKey(record.Key)
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
	return &store.Record{
		Key:   lex.EncodeOrDie(&newKey),
		Value: lex.EncodeOrDie(&newValue),
	}
}

func generateConsistentRanges(record *store.Record, outputChan chan *store.Record) {
	var intervalStartKey TraceKey
	lex.DecodeOrDie(record.Key, &intervalStartKey)
	if intervalStartKey.SequenceNumber != 0 {
		return
	}
	outputChan <- record
}
