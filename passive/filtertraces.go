package passive

import (
	"fmt"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

type filterSessions struct {
	SessionStartTime int64
	SessionEndTime   int64
}

func FilterSessionsPipeline(sessionStartTime, sessionEndTime int64, levelDbManager store.Manager, outputName string) transformer.Pipeline {
	tracesStore := levelDbManager.Reader("traces")
	traceKeyRangesStore := levelDbManager.Reader("availability-done")
	filteredStore := levelDbManager.Writer(outputName)
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
		lex.DecodeOrDie(record.Key, &session)
		if record.DatabaseIndex == 0 {
			var startKey, endKey TraceKey
			lex.DecodeOrDie(record.Key, &startKey)
			lex.DecodeOrDie(record.Value, &endKey)
			useCurrentSession = startKey.SessionId <= parameters.SessionEndTime && endKey.SessionId+traceDuration*int64(endKey.SequenceNumber) >= parameters.SessionStartTime && startKey.SequenceNumber == 0
			currentSession = &session
			continue
		}
		if useCurrentSession && session.Equal(currentSession) {
			outputChan <- record
		}
	}
}

func FilterNodesPipeline(nodeId string, levelDbManager store.Manager) transformer.Pipeline {
	tracesStore := levelDbManager.Seeker("traces")
	filteredStore := levelDbManager.Writer(fmt.Sprintf("filtered-%s", nodeId))
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:   "FilterNode",
			Reader: FilterNodes(tracesStore, nodeId),
			Writer: filteredStore,
		},
	}
}

func FilterNodes(reader store.Seeker, nodes ...string) store.Seeker {
	nodesStore := store.SliceStore{}
	nodesStore.BeginWriting()
	for _, node := range nodes {
		nodesStore.WriteRecord(&store.Record{
			Key: lex.EncodeOrDie(node),
		})
	}
	nodesStore.EndWriting()
	return store.NewPrefixIncludingReader(reader, &nodesStore)
}
