package passive

import (
	"bytes"
	"fmt"
	"log"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

type TraceKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
	SequenceNumber       int32
}

func (traceKey *TraceKey) EncodeLexicographically() ([]byte, error) {
	return lex.Encode(
		traceKey.NodeId,
		traceKey.AnonymizationContext,
		traceKey.SessionId,
		traceKey.SequenceNumber)
}

func (traceKey *TraceKey) DecodeLexicographically(reader *bytes.Buffer) error {
	return lex.Read(
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
	return lex.Encode(
		sessionKey.NodeId,
		sessionKey.AnonymizationContext,
		sessionKey.SessionId)
}

func (sessionKey *SessionKey) DecodeLexicographically(reader *bytes.Buffer) error {
	return lex.Read(
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

func TraceKeyRangesPipeline(newTraceKeysStore store.Reader, traceKeyRangesStore, consolidatedTraceKeyRangesStore store.ReadingDeleter) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "CalculateTraceKeyRanges",
			Transformer: transformer.TransformFunc(calculateTraceKeyRanges),
			Reader:      newTraceKeysStore,
			Writer:      traceKeyRangesStore,
		},
		transformer.PipelineStage{
			Name:        "ConsolidateTraceKeyRanges",
			Transformer: transformer.TransformFunc(consolidateTraceKeyRanges),
			Reader:      traceKeyRangesStore,
			Writer:      store.NewTruncatingWriter(consolidatedTraceKeyRangesStore),
		},
		transformer.PipelineStage{
			Name:   "CopyTraceKeyRanges",
			Reader: consolidatedTraceKeyRangesStore,
			Writer: store.NewTruncatingWriter(traceKeyRangesStore),
		},
	}
}

func calculateTraceKeyRanges(inputChan, outputChan chan *store.Record) {
	var firstKey, lastKey, expectedTraceKey *TraceKey
	for record := range inputChan {
		var traceKey TraceKey
		lex.DecodeOrDie(record.Key, &traceKey)
		expectedNextTraceKey := TraceKey{
			NodeId:               traceKey.NodeId,
			AnonymizationContext: traceKey.AnonymizationContext,
			SessionId:            traceKey.SessionId,
			SequenceNumber:       traceKey.SequenceNumber + 1,
		}

		if !traceKey.Equal(expectedTraceKey) {
			if firstKey != nil {
				outputChan <- &store.Record{
					Key:   lex.EncodeOrDie(firstKey),
					Value: lex.EncodeOrDie(lastKey),
				}
			}
			firstKey = &traceKey
		}
		lastKey = &traceKey
		expectedTraceKey = &expectedNextTraceKey
	}
	if firstKey != nil {
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(firstKey),
			Value: lex.EncodeOrDie(lastKey),
		}
	}
}

func consolidateTraceKeyRanges(inputChan, outputChan chan *store.Record) {
	var firstBeginKey, lastEndKey *TraceKey
	var currentSessionKey *SessionKey
	for record := range inputChan {
		var beginKey, endKey TraceKey
		lex.DecodeOrDie(record.Key, &beginKey)
		lex.DecodeOrDie(record.Value, &endKey)
		sessionKey := beginKey.SessionKey()

		if !sessionKey.Equal(currentSessionKey) || beginKey.SequenceNumber != lastEndKey.SequenceNumber+1 {
			if firstBeginKey != nil && lastEndKey != nil {
				outputChan <- &store.Record{
					Key:   lex.EncodeOrDie(firstBeginKey),
					Value: lex.EncodeOrDie(lastEndKey),
				}
			}
			firstBeginKey = &beginKey
			currentSessionKey = sessionKey
		}
		lastEndKey = &endKey
	}
	if firstBeginKey != nil && lastEndKey != nil {
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(firstBeginKey),
			Value: lex.EncodeOrDie(lastEndKey),
		}
	}
}

func SessionPipelineStage(inputStore store.Reader, sessionsStore store.Deleter) transformer.PipelineStage {
	return transformer.PipelineStage{
		Name:        "Sessions",
		Transformer: transformer.TransformFunc(sessions),
		Reader:      inputStore,
		Writer:      store.NewTruncatingWriter(sessionsStore),
	}
}

func sessions(inputChan, outputChan chan *store.Record) {
	var currentSession *SessionKey
	for record := range inputChan {
		var session SessionKey
		lex.DecodeOrDie(record.Key, &session)
		if !session.Equal(currentSession) {
			if currentSession != nil {
				outputChan <- &store.Record{
					Key: lex.EncodeOrDie(currentSession),
				}
			}
			currentSession = &session
		}
	}
	if currentSession != nil {
		outputChan <- &store.Record{
			Key: lex.EncodeOrDie(currentSession),
		}
	}
}

func PrintRecordsStage(name string, store store.Reader) transformer.PipelineStage {
	return transformer.PipelineStage{
		Name:        fmt.Sprintf("Print %v", name),
		Reader:      store,
		Transformer: transformer.TransformFunc(printRecords),
	}
}

func printRecords(inputChan, outputChan chan *store.Record) {
	log.Printf("PRINT RECORDS IN DATASTORE")
	for record := range inputChan {
		log.Printf("%s: %s (%v: %v)", record.Key, record.Value, record.Key, record.Value)
	}
	log.Printf("DONE PRINTING")
}
