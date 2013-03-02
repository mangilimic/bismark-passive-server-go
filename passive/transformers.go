package passive

import (
	"bytes"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
)

type TraceKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
	SequenceNumber       int32
}

func DecodeTraceKey(encodedKey []byte) *TraceKey {
	decodedKey := new(TraceKey)
	key.DecodeOrDie(
		encodedKey,
		&decodedKey.NodeId,
		&decodedKey.AnonymizationContext,
		&decodedKey.SessionId,
		&decodedKey.SequenceNumber)
	return decodedKey
}

func EncodeTraceKey(decodedKey *TraceKey) []byte {
	return key.EncodeOrDie(
		decodedKey.NodeId,
		decodedKey.AnonymizationContext,
		decodedKey.SessionId,
		decodedKey.SequenceNumber)
}

type SessionKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
}

func DecodeSessionKey(encodedKey []byte) *SessionKey {
	decodedKey := new(SessionKey)
	key.DecodeOrDie(
		encodedKey,
		&decodedKey.NodeId,
		&decodedKey.AnonymizationContext,
		&decodedKey.SessionId)
	return decodedKey
}

func EncodeSessionKey(decodedKey *SessionKey) []byte {
	return key.EncodeOrDie(
		decodedKey.NodeId,
		decodedKey.AnonymizationContext,
		decodedKey.SessionId)
}

func TraceKeyRangesPipeline(newTraceKeysStore transformer.StoreReader, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "CalculateTraceKeyRanges",
			Transformer: transformer.TransformFunc(CalculateTraceKeyRanges),
			Reader:      newTraceKeysStore,
			Writer:      traceKeyRangesStore,
		},
		transformer.PipelineStage{
			Name:        "ConsolidateTraceKeyRanges",
			Transformer: transformer.TransformFunc(ConsolidateTraceKeyRanges),
			Reader:      traceKeyRangesStore,
			Writer:      transformer.TruncateBeforeWriting(consolidatedTraceKeyRangesStore),
		},
		transformer.PipelineStage{
			Name:   "CopyTraceKeyRanges",
			Reader: consolidatedTraceKeyRangesStore,
			Writer: transformer.TruncateBeforeWriting(traceKeyRangesStore),
		},
	}
}

func CalculateTraceKeyRanges(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var firstKey, lastKey *TraceKey
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
			if firstKey != nil {
				outputChan <- &transformer.LevelDbRecord{
					Key:   EncodeTraceKey(firstKey),
					Value: EncodeTraceKey(lastKey),
				}
			}
			firstKey = traceKey
		}
		lastKey = traceKey
		expectedTraceKey = expectedNextTraceKey
	}
	if firstKey != nil {
		outputChan <- &transformer.LevelDbRecord{
			Key:   EncodeTraceKey(firstKey),
			Value: EncodeTraceKey(lastKey),
		}
	}
	close(outputChan)
}

func ConsolidateTraceKeyRanges(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var firstBeginKey, lastEndKey *TraceKey
	var currentSessionKey []byte
	for record := range inputChan {
		beginKey := DecodeTraceKey(record.Key)
		endKey := DecodeTraceKey(record.Value)
		sessionKey := EncodeSessionKey(&SessionKey{
			NodeId:               beginKey.NodeId,
			AnonymizationContext: beginKey.AnonymizationContext,
			SessionId:            beginKey.SessionId,
		})

		if !bytes.Equal(sessionKey, currentSessionKey) || beginKey.SequenceNumber != lastEndKey.SequenceNumber+1 {
			if firstBeginKey != nil && lastEndKey != nil {
				outputChan <- &transformer.LevelDbRecord{
					Key:   EncodeTraceKey(firstBeginKey),
					Value: EncodeTraceKey(lastEndKey),
				}
			}
			firstBeginKey = beginKey
			currentSessionKey = sessionKey
		}
		lastEndKey = endKey
	}
	if firstBeginKey != nil && lastEndKey != nil {
		outputChan <- &transformer.LevelDbRecord{
			Key:   EncodeTraceKey(firstBeginKey),
			Value: EncodeTraceKey(lastEndKey),
		}
	}
	close(outputChan)
}
