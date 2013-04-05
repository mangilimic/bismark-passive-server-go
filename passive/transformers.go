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

func (traceKey *TraceKey) EncodeLexicographically() ([]byte, error) {
	return key.Encode(
		traceKey.NodeId,
		traceKey.AnonymizationContext,
		traceKey.SessionId,
		traceKey.SequenceNumber)
}

func (traceKey *TraceKey) DecodeLexicographically(reader *bytes.Buffer) error {
	return key.Read(
		reader,
		&traceKey.NodeId,
		&traceKey.AnonymizationContext,
		&traceKey.SessionId,
		&traceKey.SequenceNumber)
}

func (traceKey *TraceKey) SessionKey() *SessionKey {
	return &SessionKey{
		NodeId:               traceKey.NodeId,
		AnonymizationContext: traceKey.AnonymizationContext,
		SessionId:            traceKey.SessionId,
	}
}

func (traceKey *TraceKey) Equal(otherTraceKey *TraceKey) bool {
	if otherTraceKey == nil {
		return false
	}
	if !bytes.Equal(traceKey.NodeId, otherTraceKey.NodeId) {
		return false
	}
	if !bytes.Equal(traceKey.AnonymizationContext, otherTraceKey.AnonymizationContext) {
		return false
	}
	if traceKey.SessionId != otherTraceKey.SessionId {
		return false
	}
	if traceKey.SequenceNumber != otherTraceKey.SequenceNumber {
		return false
	}
	return true
}

type SessionKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
}

func (sessionKey *SessionKey) EncodeLexicographically() ([]byte, error) {
	return key.Encode(
		sessionKey.NodeId,
		sessionKey.AnonymizationContext,
		sessionKey.SessionId)
}

func (sessionKey *SessionKey) DecodeLexicographically(reader *bytes.Buffer) error {
	return key.Read(
		reader,
		&sessionKey.NodeId,
		&sessionKey.AnonymizationContext,
		&sessionKey.SessionId)
}

func (sessionKey *SessionKey) TraceKey(sequenceNumber int32) *TraceKey {
	return &TraceKey{
		NodeId:               sessionKey.NodeId,
		AnonymizationContext: sessionKey.AnonymizationContext,
		SessionId:            sessionKey.SessionId,
		SequenceNumber:       sequenceNumber,
	}
}

func (sessionKey *SessionKey) Equal(otherSession *SessionKey) bool {
	if otherSession == nil {
		return false
	}
	if !bytes.Equal(sessionKey.NodeId, otherSession.NodeId) {
		return false
	}
	if !bytes.Equal(sessionKey.AnonymizationContext, otherSession.AnonymizationContext) {
		return false
	}
	if sessionKey.SessionId != otherSession.SessionId {
		return false
	}
	return true
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
	var firstKey, lastKey, expectedTraceKey *TraceKey
	for record := range inputChan {
		var traceKey TraceKey
		key.DecodeOrDie(record.Key, &traceKey)
		expectedNextTraceKey := TraceKey{
			NodeId:               traceKey.NodeId,
			AnonymizationContext: traceKey.AnonymizationContext,
			SessionId:            traceKey.SessionId,
			SequenceNumber:       traceKey.SequenceNumber + 1,
		}

		if !traceKey.Equal(expectedTraceKey) {
			if firstKey != nil {
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(firstKey),
					Value: key.EncodeOrDie(lastKey),
				}
			}
			firstKey = &traceKey
		}
		lastKey = &traceKey
		expectedTraceKey = &expectedNextTraceKey
	}
	if firstKey != nil {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(firstKey),
			Value: key.EncodeOrDie(lastKey),
		}
	}
	close(outputChan)
}

func ConsolidateTraceKeyRanges(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var firstBeginKey, lastEndKey *TraceKey
	var currentSessionKey *SessionKey
	for record := range inputChan {
		var beginKey, endKey TraceKey
		key.DecodeOrDie(record.Key, &beginKey)
		key.DecodeOrDie(record.Value, &endKey)
		sessionKey := beginKey.SessionKey()

		if !sessionKey.Equal(currentSessionKey) || beginKey.SequenceNumber != lastEndKey.SequenceNumber+1 {
			if firstBeginKey != nil && lastEndKey != nil {
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(firstBeginKey),
					Value: key.EncodeOrDie(lastEndKey),
				}
			}
			firstBeginKey = &beginKey
			currentSessionKey = sessionKey
		}
		lastEndKey = &endKey
	}
	if firstBeginKey != nil && lastEndKey != nil {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(firstBeginKey),
			Value: key.EncodeOrDie(lastEndKey),
		}
	}
	close(outputChan)
}

func Sessions(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSession *SessionKey
	for record := range inputChan {
		var session SessionKey
		key.DecodeOrDie(record.Key, &session)
		if !session.Equal(currentSession) {
			if currentSession != nil {
				outputChan <- &transformer.LevelDbRecord{
					Key: key.EncodeOrDie(currentSession),
				}
			}
			currentSession = &session
		}
	}
	if currentSession != nil {
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(currentSession),
		}
	}
	close(outputChan)
}

func SessionPipelineStage(inputStore transformer.StoreReader, sessionsStore transformer.StoreDeleter) transformer.PipelineStage {
	return transformer.PipelineStage{
		Name:        "Sessions",
		Transformer: transformer.TransformFunc(Sessions),
		Reader:      inputStore,
		Writer:      transformer.TruncateBeforeWriting(sessionsStore),
	}
}
