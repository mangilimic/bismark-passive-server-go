package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
)

func makeTraceWithStatistics(nodeId string, packetSizes []int, packetsDropped, pcapDropped, interfaceDropped, numFlows, flowsDropped int) *Trace {
	trace := &Trace{
		NodeId:              &nodeId,
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

func runAggregateStatisticsPipeline(allTraces ...[]*Trace) {
	tracesStore := transformer.SliceStore{}
	traceAggregatesStore := transformer.SliceStore{}
	nodeAggregatesStore := transformer.SliceStore{}
	var writer *bytes.Buffer
	traceKeyRangesStore := transformer.SliceStore{}
	consolidatedTraceKeyRangesStore := transformer.SliceStore{}
	sequenceNumber := int32(0)
	for _, traces := range allTraces {
		tracesStore.BeginWriting()
		for _, trace := range traces {
			traceKey := TraceKey{
				NodeId:               []byte(*trace.NodeId),
				AnonymizationContext: []byte("context"),
				SessionId:            0,
				SequenceNumber:       sequenceNumber,
			}
			encodedKey := key.EncodeOrDie(&traceKey)
			encodedTrace, err := proto.Marshal(trace)
			if err != nil {
				panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
			}
			tracesStore.WriteRecord(&transformer.LevelDbRecord{Key: encodedKey, Value: encodedTrace})
			sequenceNumber++
		}
		tracesStore.EndWriting()

		writer = bytes.NewBuffer([]byte{})

		transformer.RunPipeline(AggregateStatisticsPipeline(&tracesStore, &traceAggregatesStore, &nodeAggregatesStore, writer, &traceKeyRangesStore, &consolidatedTraceKeyRangesStore, 1), 0)
	}
	fmt.Printf("%s", writer.Bytes())
}

func ExampleAggregateStatisticsPipeline() {
	runAggregateStatisticsPipeline([]*Trace{
		makeTraceWithStatistics("node", []int{2, 2, 2}, 1, 0, 0, 2, 3),
		makeTraceWithStatistics("node", []int{1, 2}, 0, 1, 0, 3, 4),
		makeTraceWithStatistics("node", []int{1, 2}, 0, 0, 1, 4, 5),
	})

	// Output:
	// [["node",3,7,3,9,12,12]]
}

func ExampleAggregateStatisticsPipeline_multipleNodes() {
	runAggregateStatisticsPipeline([]*Trace{
		makeTraceWithStatistics("node1", []int{2, 2, 2}, 1, 0, 0, 2, 3),
		makeTraceWithStatistics("node1", []int{1, 2}, 0, 1, 0, 3, 4),
		makeTraceWithStatistics("node2", []int{1, 2}, 0, 0, 1, 4, 5),
		makeTraceWithStatistics("node2", []int{1, 2}, 0, 0, 1, 4, 5),
	})

	// Output:
	// [["node1",2,5,2,5,7,9],["node2",2,4,2,8,10,6]]
}

func ExampleAggregateStatisticsPipeline_multipleRuns() {
	runAggregateStatisticsPipeline([]*Trace{
		makeTraceWithStatistics("node", []int{2, 2, 2}, 1, 0, 0, 2, 3),
		makeTraceWithStatistics("node", []int{1, 2}, 0, 1, 0, 3, 4),
	}, []*Trace{
		makeTraceWithStatistics("node", []int{1, 2}, 0, 0, 1, 4, 5),
	})

	// Output:
	// [["node",3,7,3,9,12,12]]
}
