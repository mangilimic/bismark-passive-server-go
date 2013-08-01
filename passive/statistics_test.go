package passive

import (
	"bytes"
	"fmt"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"github.com/sburnett/transformer/store"
)

func makeTraceWithStatistics(packetSizes []int, packetsDropped, pcapDropped, interfaceDropped, numFlows, flowsDropped int) Trace {
	trace := Trace{
		PacketSeries:        make([]*PacketSeriesEntry, len(packetSizes)),
		PacketSeriesDropped: proto.Uint32(uint32(packetsDropped)),
		PcapDropped:         proto.Uint32(uint32(pcapDropped)),
		InterfaceDropped:    proto.Uint32(uint32(interfaceDropped)),
		FlowTableEntry:      make([]*FlowTableEntry, numFlows),
		FlowTableDropped:    proto.Int32(int32(flowsDropped)),
	}
	for idx := 0; idx < numFlows; idx++ {
		trace.FlowTableEntry[idx] = &FlowTableEntry{}
	}
	for idx, packetSize := range packetSizes {
		trace.PacketSeries[idx] = &PacketSeriesEntry{
			Size: proto.Int32(int32(packetSize)),
		}
	}
	return trace
}

func runAggregateStatisticsPipeline(consistentRanges []*store.Record, allTraces ...map[string]Trace) {
	tracesStore := store.SliceStore{}
	availabilityIntervalsStore := store.SliceStore{}
	availabilityIntervalsStore.BeginWriting()
	for _, record := range consistentRanges {
		availabilityIntervalsStore.WriteRecord(record)
	}
	availabilityIntervalsStore.EndWriting()
	traceAggregatesStore := store.SliceStore{}
	sessionAggregatesStore := store.SliceStore{}
	nodeAggregatesStore := store.SliceStore{}
	var writer *bytes.Buffer
	sessionsStore := store.SliceStore{}
	traceKeyRangesStore := store.SliceStore{}
	consolidatedTraceKeyRangesStore := store.SliceStore{}
	for _, traces := range allTraces {
		tracesStore.BeginWriting()
		for encodedKey, trace := range traces {
			encodedTrace, err := proto.Marshal(&trace)
			if err != nil {
				panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
			}
			tracesStore.WriteRecord(&store.Record{Key: []byte(encodedKey), Value: encodedTrace})
		}
		tracesStore.EndWriting()

		writer = bytes.NewBuffer([]byte{})

		transformer.RunPipeline(AggregateStatisticsPipeline(&tracesStore, &availabilityIntervalsStore, &traceAggregatesStore, &sessionAggregatesStore, &nodeAggregatesStore, writer, &sessionsStore, &traceKeyRangesStore, &consolidatedTraceKeyRangesStore, 1))
	}
	fmt.Printf("%s", writer.Bytes())
}

func ExampleAggregateStatisticsPipeline() {
	consistentRanges := []*store.Record{
		&store.Record{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(2)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): makeTraceWithStatistics([]int{2, 2, 2}, 1, 0, 0, 2, 3),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): makeTraceWithStatistics([]int{1, 2}, 0, 1, 0, 3, 4),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): makeTraceWithStatistics([]int{1, 2}, 0, 1, 1, 4, 5),
	}
	runAggregateStatisticsPipeline(consistentRanges, records)

	// Output:
	// [["node0",3,7,3,9,12,12]]
}

func ExampleAggregateStatisticsPipeline_multipleNodes() {
	consistentRanges := []*store.Record{
		&store.Record{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
		&store.Record{
			Key:   key.EncodeOrDie("node1", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node1", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): makeTraceWithStatistics([]int{2, 2, 2}, 1, 0, 0, 2, 3),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): makeTraceWithStatistics([]int{1, 2}, 0, 1, 0, 3, 4),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(0))): makeTraceWithStatistics([]int{1, 2}, 0, 0, 1, 4, 5),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(1))): makeTraceWithStatistics([]int{1, 2}, 0, 0, 1, 4, 5),
	}
	runAggregateStatisticsPipeline(consistentRanges, records)

	// Output:
	// [["node0",2,5,2,5,7,9],["node1",2,4,1,8,10,6]]
}

func ExampleAggregateStatisticsPipeline_multipleSessions() {
	consistentRanges := []*store.Record{
		&store.Record{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
		&store.Record{
			Key:   key.EncodeOrDie("node0", "anon0", int64(1), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(1), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): makeTraceWithStatistics([]int{2, 2, 2}, 1, 0, 0, 2, 3),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): makeTraceWithStatistics([]int{1, 2}, 0, 1, 0, 3, 4),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(0))): makeTraceWithStatistics([]int{1, 2}, 0, 0, 1, 4, 5),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(1))): makeTraceWithStatistics([]int{1, 2}, 0, 0, 1, 4, 5),
	}
	runAggregateStatisticsPipeline(consistentRanges, records)

	// Output:
	// [["node0",4,9,3,13,17,15]]
}

func ExampleAggregateStatisticsPipeline_multipleRuns() {
	consistentRanges := []*store.Record{
		&store.Record{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(2)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): makeTraceWithStatistics([]int{2, 2, 2}, 1, 0, 0, 2, 3),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): makeTraceWithStatistics([]int{1, 2}, 0, 1, 0, 3, 4),
	}
	moreRecords := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): makeTraceWithStatistics([]int{1, 2}, 0, 1, 1, 4, 5),
	}
	runAggregateStatisticsPipeline(consistentRanges, records, moreRecords)

	// Output:
	// [["node0",3,7,3,9,12,12]]
}
