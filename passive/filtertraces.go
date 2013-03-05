package passive

import (
	"bytes"
	"github.com/sburnett/transformer"
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
	var currentSession []byte
	for record := range inputChan {
		session := EncodeSessionKey(DecodeSessionKey(record.Key))
		if record.DatabaseIndex == 0 {
			startKey := DecodeTraceKey(record.Key)
			endKey := DecodeTraceKey(record.Value)
			useCurrentSession = startKey.SessionId <= parameters.SessionEndTime && endKey.SessionId+traceDuration*int64(endKey.SequenceNumber) >= parameters.SessionStartTime && startKey.SequenceNumber == 0
			currentSession = session
			continue
		}
		if useCurrentSession && bytes.Equal(session, currentSession) {
			outputChan <- record
		}
	}
	close(outputChan)
}
