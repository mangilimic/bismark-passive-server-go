package passive

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

const LogDuringTests bool = false

func init() {
	if !LogDuringTests {
		DisableLogging()
	}
}

func EnableLogging() {
	log.SetOutput(os.Stderr)
}

func DisableLogging() {
	log.SetOutput(ioutil.Discard)
}

func formatSessionKey(sessionKey []byte) string {
	var decoded SessionKey
	lex.DecodeOrDie(sessionKey, &decoded)
	return fmt.Sprintf("%s,%s,%d", decoded.NodeId, decoded.AnonymizationContext, decoded.SessionId)
}

func runSessions(records []*store.Record) {
	traces := store.SliceStore{}
	traces.BeginWriting()
	for _, record := range records {
		traces.WriteRecord(record)
	}
	traces.EndWriting()

	sessionsStore := store.SliceStore{}
	transformer.RunPipeline([]transformer.PipelineStage{
		SessionPipelineStage(&traces, &sessionsStore),
	})

	sessionsStore.BeginReading()
	for {
		record, err := sessionsStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s\n", formatSessionKey(record.Key))
	}
	sessionsStore.EndReading()
}

func makeTraceKey(nodeId, anonymizationContext string, sessionId, sequenceNumber int) *store.Record {
	traceKey := TraceKey{
		NodeId:               []byte(nodeId),
		AnonymizationContext: []byte(anonymizationContext),
		SessionId:            int64(sessionId),
		SequenceNumber:       int32(sequenceNumber),
	}
	return &store.Record{
		Key: lex.EncodeOrDie(&traceKey),
	}
}

func formatTraceKey(traceKey []byte) string {
	var decoded TraceKey
	lex.DecodeOrDie(traceKey, &decoded)
	return fmt.Sprintf("%s,%s,%d,%d", decoded.NodeId, decoded.AnonymizationContext, decoded.SessionId, decoded.SequenceNumber)
}

func runCalculateTraceKeyRanges(records []*store.Record) {
	traces := store.SliceStore{}
	traces.BeginWriting()
	for _, record := range records {
		traces.WriteRecord(record)
	}
	traces.EndWriting()

	rangesStore := store.SliceStore{}
	consolidatedStore := store.SliceStore{}
	transformer.RunPipeline(TraceKeyRangesPipeline(&traces, &rangesStore, &consolidatedStore))

	rangesStore.BeginReading()
	for {
		record, err := rangesStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s %s\n", formatTraceKey(record.Key), formatTraceKey(record.Value))
	}
	rangesStore.EndReading()
}

func runConsolidateTraceKeyRanges(records, moreRecords []*store.Record) {
	traces := store.SliceStore{}
	traces.BeginWriting()
	for _, record := range records {
		traces.WriteRecord(record)
	}
	traces.EndWriting()

	rangesStore := store.SliceStore{}
	consolidatedStore := store.SliceStore{}
	transformer.RunPipeline(TraceKeyRangesPipeline(&traces, &rangesStore, &consolidatedStore))

	moreTraces := store.SliceStore{}
	moreTraces.BeginWriting()
	for _, record := range moreRecords {
		moreTraces.WriteRecord(record)
	}
	moreTraces.EndWriting()

	transformer.RunPipeline(TraceKeyRangesPipeline(&moreTraces, &rangesStore, &consolidatedStore))

	rangesStore.BeginReading()
	for {
		record, err := rangesStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s %s\n", formatTraceKey(record.Key), formatTraceKey(record.Value))
	}
	rangesStore.EndReading()
}

func ExampleTraceKeyTraces() {
	runCalculateTraceKeyRanges([]*store.Record{
		makeTraceKey("node", "context", 10, 0),
		makeTraceKey("node", "context", 10, 1),
		makeTraceKey("node", "context", 10, 3),
		makeTraceKey("node", "context", 10, 4),
		makeTraceKey("node", "context", 10, 5),
		makeTraceKey("node", "context", 10, 7),
	})

	// Output:
	// node,context,10,0 node,context,10,1
	// node,context,10,3 node,context,10,5
	// node,context,10,7 node,context,10,7
}

func ExampleTraceKeyTraces_multipleSesssions() {
	runCalculateTraceKeyRanges([]*store.Record{
		makeTraceKey("node0", "context1", 10, 0),
		makeTraceKey("node1", "context1", 10, 1),
		makeTraceKey("node1", "context2", 10, 2),
		makeTraceKey("node1", "context2", 11, 3),
	})

	// Output:
	// node0,context1,10,0 node0,context1,10,0
	// node1,context1,10,1 node1,context1,10,1
	// node1,context2,10,2 node1,context2,10,2
	// node1,context2,11,3 node1,context2,11,3
}

func ExampleTraceKeyTraces_multipleRounds() {
	runConsolidateTraceKeyRanges([]*store.Record{
		makeTraceKey("node", "context", 10, 0),
		makeTraceKey("node", "context", 10, 1),
		makeTraceKey("node", "context", 10, 3),
		makeTraceKey("node", "context", 10, 4),
		makeTraceKey("node", "context", 10, 5),
		makeTraceKey("node", "context", 10, 7),
	}, []*store.Record{
		makeTraceKey("node", "context", 10, 2),
		makeTraceKey("node", "context", 10, 6),
	})

	// Output:
	// node,context,10,0 node,context,10,7
}

func ExampleTraceKeyTraces_multipleRoundsWithHoles() {
	runConsolidateTraceKeyRanges([]*store.Record{
		makeTraceKey("node", "context", 10, 0),
		makeTraceKey("node", "context", 10, 1),
		makeTraceKey("node", "context", 10, 3),
		makeTraceKey("node", "context", 10, 5),
		makeTraceKey("node", "context", 10, 7),
	}, []*store.Record{
		makeTraceKey("node", "context", 10, 2),
		makeTraceKey("node", "context", 10, 6),
	})

	// Output:
	// node,context,10,0 node,context,10,3
	// node,context,10,5 node,context,10,7
}

func ExampleSessions() {
	runSessions([]*store.Record{
		makeTraceKey("node", "context", 0, 0),
		makeTraceKey("node", "context", 0, 1),
		makeTraceKey("node", "context", 0, 2),
		makeTraceKey("node", "context", 10, 0),
		makeTraceKey("node2", "context", 0, 2),
		makeTraceKey("node2", "context", 0, 2),
		makeTraceKey("node", "context2", 0, 2),
		makeTraceKey("node", "context2", 0, 2),
	})

	// Output:
	// node,context,0
	// node,context,10
	// node,context2,0
	// node2,context,0
}
