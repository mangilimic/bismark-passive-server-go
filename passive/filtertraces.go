package passive

import (
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
)

type FilterSessions struct {
	SessionStartTime int64
	SessionEndTime   int64
}

func FilterSessionsPipeline(sessionStartTime, sessionEndTime int64, tracesStore, traceKeyRangesStore transformer.StoreReader, filteredStore transformer.StoreWriter) []transformer.PipelineStage {
	parameters := FilterSessions{
		SessionStartTime: sessionStartTime * 1000000,
		SessionEndTime:   sessionEndTime * 1000000,
	}
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "FilterSessions",
			Reader:      transformer.NewDemuxStoreReader(traceKeyRangesStore, tracesStore),
			Transformer: parameters,
			Writer:      filteredStore,
		},
	}
}

func (parameters FilterSessions) Do(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
