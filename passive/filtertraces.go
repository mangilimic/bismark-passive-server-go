package passive

import (
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"github.com/sburnett/transformer/store"
)

type filterSessions struct {
	SessionStartTime int64
	SessionEndTime   int64
}

func FilterSessionsPipeline(sessionStartTime, sessionEndTime int64, tracesStore, traceKeyRangesStore store.Reader, filteredStore store.Writer) []transformer.PipelineStage {
	parameters := filterSessions{
		SessionStartTime: sessionStartTime * 1000000,
		SessionEndTime:   sessionEndTime * 1000000,
	}
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "FilterSessions",
			Reader:      store.NewDemuxingReader(traceKeyRangesStore, tracesStore),
			Transformer: parameters,
			Writer:      filteredStore,
		},
	}
}

func (parameters filterSessions) Do(inputChan, outputChan chan *store.Record) {
	traceDuration := int64(30000000) // This is a big assumption about how clients generate traces.
	var useCurrentSession bool
	var currentSession *SessionKey
	for record := range inputChan {
		var session SessionKey
		key.DecodeOrDie(record.Key, &session)
		if record.DatabaseIndex == 0 {
			var startKey, endKey TraceKey
			key.DecodeOrDie(record.Key, &startKey)
			key.DecodeOrDie(record.Value, &endKey)
			useCurrentSession = startKey.SessionId <= parameters.SessionEndTime && endKey.SessionId+traceDuration*int64(endKey.SequenceNumber) >= parameters.SessionStartTime && startKey.SequenceNumber == 0
			currentSession = &session
			continue
		}
		if useCurrentSession && session.Equal(currentSession) {
			outputChan <- record
		}
	}
}
