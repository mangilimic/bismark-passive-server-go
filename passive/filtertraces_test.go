package passive

import (
	"fmt"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func makeSessionRecord(nodeId string, sessionId int64, sequenceNumber int32) *store.Record {
	traceKey := TraceKey{
		NodeId:               []byte(nodeId),
		AnonymizationContext: []byte("context"),
		SessionId:            sessionId,
		SequenceNumber:       sequenceNumber,
	}
	return &store.Record{
		Key:   lex.EncodeOrDie(&traceKey),
		Value: []byte{},
	}
}

func makeRangeRecord(nodeId string, sessionId int64, firstSequenceNumber, lastSequenceNumber int32) *store.Record {
	traceKey := TraceKey{
		NodeId:               []byte(nodeId),
		AnonymizationContext: []byte("context"),
		SessionId:            sessionId,
		SequenceNumber:       firstSequenceNumber,
	}
	traceValue := TraceKey{
		NodeId:               []byte(nodeId),
		AnonymizationContext: []byte("context"),
		SessionId:            sessionId,
		SequenceNumber:       lastSequenceNumber,
	}
	return &store.Record{
		Key:   lex.EncodeOrDie(&traceKey),
		Value: lex.EncodeOrDie(&traceValue),
	}
}

func runFilterSessionsPipeline(startSecs, endSecs int64, levelDbManager store.Manager) {

	transformer.RunPipeline(FilterSessionsPipeline(startSecs, endSecs, levelDbManager, "test"))

	filteredStore := levelDbManager.Reader("test")
	filteredStore.BeginReading()
	for {
		record, err := filteredStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		var traceKey TraceKey
		lex.DecodeOrDie(record.Key, &traceKey)
		fmt.Printf("%s %d %d\n", traceKey.NodeId, traceKey.SessionId, traceKey.SequenceNumber)
	}
	filteredStore.EndReading()
}

func ExampleFilterSessions() {
	usecs := int64(1000000)

	levelDbManager := store.NewSliceManager()

	traceKeyRangesStore := levelDbManager.Writer("availability-done")
	traceKeyRangesStore.BeginWriting()
	traceKeyRangesStore.WriteRecord(makeRangeRecord("node", 30*usecs, 0, 2))
	traceKeyRangesStore.WriteRecord(makeRangeRecord("node", 31*usecs, 0, 1))
	traceKeyRangesStore.WriteRecord(makeRangeRecord("node", 100*usecs, 0, 10))
	traceKeyRangesStore.WriteRecord(makeRangeRecord("node", 200*usecs, 2, 8))
	traceKeyRangesStore.BeginWriting()

	tracesStore := levelDbManager.Writer("traces")
	tracesStore.BeginWriting()
	tracesStore.WriteRecord(makeSessionRecord("node", 30*usecs, 1))
	tracesStore.WriteRecord(makeSessionRecord("node", 31*usecs, 3))
	tracesStore.WriteRecord(makeSessionRecord("node", 100*usecs, 2))
	tracesStore.WriteRecord(makeSessionRecord("node", 200*usecs, 3))
	tracesStore.EndWriting()

	runFilterSessionsPipeline(80, 120, levelDbManager)

	// Output:
	// node 30000000 1
	// node 100000000 2
}

func makeRecordToInclude(nodeId string, sequenceNumber int32) *store.Record {
	traceKey := TraceKey{
		NodeId:               []byte(nodeId),
		AnonymizationContext: []byte("context"),
		SessionId:            0,
		SequenceNumber:       sequenceNumber,
	}
	return &store.Record{
		Key:   lex.EncodeOrDie(&traceKey),
		Value: []byte{},
	}
}

func runIncludeNodes(records store.Reader) {
	records.BeginReading()
	for {
		record, err := records.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		var traceKey TraceKey
		lex.DecodeOrDie(record.Key, &traceKey)
		fmt.Printf("%s %d\n", traceKey.NodeId, traceKey.SequenceNumber)
	}
	records.EndReading()
}

func ExampleIncludeNodes() {
	inputRecords := store.SliceStore{}
	inputRecords.BeginWriting()
	inputRecords.WriteRecord(makeRecordToInclude("node1", 1))
	inputRecords.WriteRecord(makeRecordToInclude("node1", 2))
	inputRecords.WriteRecord(makeRecordToInclude("node2", 3))
	inputRecords.WriteRecord(makeRecordToInclude("node2", 4))
	inputRecords.WriteRecord(makeRecordToInclude("node2", 5))
	inputRecords.WriteRecord(makeRecordToInclude("node3", 6))
	inputRecords.EndWriting()

	outputRecords := FilterNodes(&inputRecords, "node2")
	runIncludeNodes(outputRecords)

	// Output:
	// node2 3
	// node2 4
	// node2 5
}

func ExampleIncludeNodes_multipleNodes() {
	inputRecords := store.SliceStore{}
	inputRecords.BeginWriting()
	inputRecords.WriteRecord(makeRecordToInclude("node1", 1))
	inputRecords.WriteRecord(makeRecordToInclude("node1", 2))
	inputRecords.WriteRecord(makeRecordToInclude("node2", 3))
	inputRecords.WriteRecord(makeRecordToInclude("node2", 4))
	inputRecords.WriteRecord(makeRecordToInclude("node2", 5))
	inputRecords.WriteRecord(makeRecordToInclude("node3", 6))
	inputRecords.WriteRecord(makeRecordToInclude("node3", 7))
	inputRecords.WriteRecord(makeRecordToInclude("node4", 8))
	inputRecords.WriteRecord(makeRecordToInclude("node4", 9))
	inputRecords.WriteRecord(makeRecordToInclude("node5", 10))
	inputRecords.WriteRecord(makeRecordToInclude("node6", 11))
	inputRecords.WriteRecord(makeRecordToInclude("node7", 12))
	inputRecords.EndWriting()

	outputRecords := FilterNodes(&inputRecords, "node2", "node4", "node5", "node7")
	runIncludeNodes(outputRecords)

	// Output:
	// node2 3
	// node2 4
	// node2 5
	// node4 8
	// node4 9
	// node5 10
	// node7 12
}
