package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"io"
)

func AggregateStatisticsPipeline(tracesStore transformer.StoreSeeker, traceAggregatesStore transformer.DatastoreFull, nodeAggregatesStore transformer.Datastore, jsonWriter io.Writer, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull, workers int) []transformer.PipelineStage {
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "AggregateStatisticsMapper",
			Transformer: transformer.MakeMapFunc(AggregateStatisticsMapper, workers),
			Reader:      transformer.ReadExcludingRanges(tracesStore, traceKeyRangesStore),
			Writer:      transformer.TruncateBeforeWriting(traceAggregatesStore),
		},
		transformer.PipelineStage{
			Name:        "AggregateStatisticsReducer",
			Transformer: transformer.TransformFunc(AggregateStatisticsReducer),
			Reader:      transformer.NewDemuxStoreReader(traceAggregatesStore, nodeAggregatesStore),
			Writer:      nodeAggregatesStore,
		},
		transformer.PipelineStage{
			Name:   "AggregateStatisticsJson",
			Reader: nodeAggregatesStore,
			Writer: &AggregateStatisticsJsonStore{writer: jsonWriter},
		},
	}, TraceKeyRangesPipeline(traceAggregatesStore, traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

func AggregateStatisticsMapper(record *transformer.LevelDbRecord) *transformer.LevelDbRecord {
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}
	byteCount := int64(0)
	for _, packetEntry := range trace.PacketSeries {
		byteCount += int64(*packetEntry.Size)
	}
	statistics := AggregateStatistics{
		Traces:         proto.Int64(1),
		Packets:        proto.Int64(int64(len(trace.PacketSeries))),
		DroppedPackets: proto.Int64(int64(*trace.PacketSeriesDropped + *trace.PcapDropped + *trace.InterfaceDropped)),
		Flows:          proto.Int64(int64(len(trace.FlowTableEntry))),
		DroppedFlows:   proto.Int64(int64(*trace.FlowTableDropped)),
		Bytes:          &byteCount,
	}
	encodedStatistics, err := proto.Marshal(&statistics)
	if err != nil {
		panic(err)
	}
	return &transformer.LevelDbRecord{
		Key:   record.Key,
		Value: encodedStatistics,
	}
}

func newAggregateStatistics() *AggregateStatistics {
	return &AggregateStatistics{
		Traces:         proto.Int64(0),
		Packets:        proto.Int64(0),
		DroppedPackets: proto.Int64(0),
		Flows:          proto.Int64(0),
		DroppedFlows:   proto.Int64(0),
		Bytes:          proto.Int64(0),
	}
}

func mergeAggregateStatistics(source, destination *AggregateStatistics) {
	*destination.Traces += *source.Traces
	*destination.Packets += *source.Packets
	*destination.DroppedPackets += *source.DroppedPackets
	*destination.Flows += *source.Flows
	*destination.DroppedFlows += *source.DroppedFlows
	*destination.Bytes += *source.Bytes
}

func AggregateStatisticsReducer(inputChan, outputChan chan *transformer.LevelDbRecord) {
	writeNodeStatistics := func(nodeId []byte, statistics *AggregateStatistics) {
		encodedStatistics, err := proto.Marshal(statistics)
		if err != nil {
			panic(err)
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(nodeId),
			Value: encodedStatistics,
		}
	}

	var currentNodeId []byte
	var currentStatistics *AggregateStatistics
	for record := range inputChan {
		var nodeId []byte
		key.DecodeOrDie(record.Key, &nodeId)
		if !bytes.Equal(nodeId, currentNodeId) {
			if currentStatistics != nil {
				writeNodeStatistics(currentNodeId, currentStatistics)
			}
			currentNodeId = nodeId
			currentStatistics = newAggregateStatistics()
		}

		var statistics AggregateStatistics
		if err := proto.Unmarshal(record.Value, &statistics); err != nil {
			panic(err)
		}
		mergeAggregateStatistics(&statistics, currentStatistics)
	}
	if currentNodeId != nil && currentStatistics != nil {
		writeNodeStatistics(currentNodeId, currentStatistics)
	}
	close(outputChan)
}

type AggregateStatisticsJsonStore struct {
	writer io.Writer
	first  bool
}

func (store *AggregateStatisticsJsonStore) BeginWriting() error {
	if _, err := fmt.Fprintf(store.writer, "["); err != nil {
		return err
	}
	store.first = true
	return nil
}

func (store *AggregateStatisticsJsonStore) WriteRecord(record *transformer.LevelDbRecord) error {
	var nodeId string
	key.DecodeOrDie(record.Key, &nodeId)
	if store.first {
		store.first = false
	} else {
		if _, err := fmt.Fprintf(store.writer, ","); err != nil {
			return err
		}
	}
	var statistics AggregateStatistics
	if err := proto.Unmarshal(record.Value, &statistics); err != nil {
		panic(err)
	}
	if _, err := fmt.Fprintf(store.writer, "[%q,%d,%d,%d,%d,%d,%d]", nodeId, *statistics.Traces, *statistics.Packets, *statistics.DroppedPackets, *statistics.Flows, *statistics.DroppedFlows, *statistics.Bytes); err != nil {
		return err
	}
	return nil
}

func (store *AggregateStatisticsJsonStore) EndWriting() error {
	if _, err := fmt.Fprintf(store.writer, "]"); err != nil {
		return err
	}
	return nil
}
