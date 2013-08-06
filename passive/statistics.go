package passive

import (
	"fmt"
	"io"
	"math"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func AggregateStatisticsPipeline(levelDbManager store.Manager, jsonWriter io.Writer, workers int) transformer.Pipeline {
	tracesStore := levelDbManager.Seeker("traces")
	availabilityIntervalsStore := levelDbManager.Seeker("consistent-ranges")
	traceAggregatesStore := levelDbManager.SeekingWriter("statistics-trace-aggregates")
	sessionAggregatesStore := levelDbManager.ReadingWriter("statistics-session-aggregates")
	nodeAggregatesStore := levelDbManager.ReadingWriter("statistics-node-aggregates")
	sessionsStore := levelDbManager.ReadingDeleter("statistics-sessions")
	traceKeyRangesStore := levelDbManager.ReadingDeleter("statistics-trace-key-ranges")
	consolidatedTraceKeyRangesStore := levelDbManager.ReadingDeleter("statistics-consolidated-trace-key-ranges")
	newTracesStore := store.NewRangeExcludingReader(store.NewRangeIncludingReader(tracesStore, availabilityIntervalsStore), traceKeyRangesStore)
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "AggregateStatisticsMapper",
			Reader:      store.NewRangeExcludingReader(tracesStore, traceKeyRangesStore),
			Transformer: transformer.MakeMapFunc(aggregateStatisticsMapper, workers),
			Writer:      traceAggregatesStore,
		},
		SessionPipelineStage(newTracesStore, sessionsStore),
		transformer.PipelineStage{
			Name:        "AggregateStatisticsReduceBySession",
			Reader:      store.NewPrefixIncludingReader(traceAggregatesStore, sessionsStore),
			Transformer: transformer.TransformFunc(aggregateStatisticsReduceBySession),
			Writer:      sessionAggregatesStore,
		},
		transformer.PipelineStage{
			Name:        "AggregateStatisticsReducer",
			Reader:      sessionAggregatesStore,
			Transformer: transformer.TransformFunc(aggregateStatisticsReducer),
			Writer:      nodeAggregatesStore,
		},
		transformer.PipelineStage{
			Name:   "AggregateStatisticsJson",
			Reader: nodeAggregatesStore,
			Writer: &aggregateStatisticsJsonStore{writer: jsonWriter},
		},
	}, TraceKeyRangesPipeline(newTracesStore, traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

func aggregateStatisticsMapper(record *store.Record) *store.Record {
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}
	byteCount := int64(0)
	for _, packetEntry := range trace.PacketSeries {
		byteCount += int64(*packetEntry.Size)
	}
	statistics := AggregateStatistics{
		Traces:              proto.Int64(1),
		Packets:             proto.Int64(int64(len(trace.PacketSeries))),
		PacketSeriesDropped: proto.Int64(int64(*trace.PacketSeriesDropped)),
		PcapDropped:         proto.Int64(int64(*trace.PcapDropped)),
		InterfaceDropped:    proto.Int64(int64(*trace.InterfaceDropped)),
		Flows:               proto.Int64(int64(len(trace.FlowTableEntry))),
		DroppedFlows:        proto.Int64(int64(*trace.FlowTableDropped)),
		Bytes:               &byteCount,
	}
	encodedStatistics, err := proto.Marshal(&statistics)
	if err != nil {
		panic(err)
	}
	return &store.Record{
		Key:   record.Key,
		Value: encodedStatistics,
	}
}

func newAggregateStatistics() *AggregateStatistics {
	return &AggregateStatistics{
		Traces:              proto.Int64(0),
		Packets:             proto.Int64(0),
		PacketSeriesDropped: proto.Int64(0),
		PcapDropped:         proto.Int64(0),
		InterfaceDropped:    proto.Int64(0),
		Flows:               proto.Int64(0),
		DroppedFlows:        proto.Int64(0),
		Bytes:               proto.Int64(0),
	}
}

func mergeAggregateStatistics(source, destination *AggregateStatistics) {
	*destination.Traces += *source.Traces
	*destination.Packets += *source.Packets
	*destination.PacketSeriesDropped += *source.PacketSeriesDropped
	*destination.PcapDropped = maxInt64(*destination.PcapDropped, *source.PcapDropped)
	*destination.InterfaceDropped = maxInt64(*destination.InterfaceDropped, *source.InterfaceDropped)
	*destination.Flows += *source.Flows
	*destination.DroppedFlows += *source.DroppedFlows
	*destination.Bytes += *source.Bytes
}

func aggregateStatisticsReduceBySession(inputChan, outputChan chan *store.Record) {
	var session SessionKey
	grouper := transformer.GroupRecords(inputChan, &session)
	for grouper.NextGroup() {
		aggregateStatistics := newAggregateStatistics()
		var pcapDropped, interfaceDropped int64
		var lastPcapDropped, lastInterfaceDropped int64
		var pcapDroppedBaseline, interfaceDroppedBaseline int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var statistics AggregateStatistics
			if err := proto.Unmarshal(record.Value, &statistics); err != nil {
				panic(err)
			}

			if lastPcapDropped > *statistics.PcapDropped {
				pcapDroppedBaseline += math.MaxUint32
				pcapDropped = 0
			}
			lastPcapDropped = *statistics.PcapDropped
			pcapDropped = maxInt64(pcapDropped, *statistics.PcapDropped)
			if lastInterfaceDropped > *statistics.InterfaceDropped {
				interfaceDroppedBaseline += math.MaxUint32
				interfaceDropped = 0
			}
			lastInterfaceDropped = *statistics.InterfaceDropped
			interfaceDropped = maxInt64(interfaceDropped, *statistics.InterfaceDropped)

			*aggregateStatistics.Traces += *statistics.Traces
			*aggregateStatistics.Packets += *statistics.Packets
			*aggregateStatistics.PacketSeriesDropped += *statistics.PacketSeriesDropped
			*aggregateStatistics.Flows += *statistics.Flows
			*aggregateStatistics.DroppedFlows += *statistics.DroppedFlows
			*aggregateStatistics.Bytes += *statistics.Bytes
		}

		*aggregateStatistics.PcapDropped = pcapDroppedBaseline + pcapDropped
		*aggregateStatistics.InterfaceDropped = interfaceDroppedBaseline + interfaceDropped

		encodedStatistics, err := proto.Marshal(aggregateStatistics)
		if err != nil {
			panic(err)
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(&session),
			Value: encodedStatistics,
		}
	}
}

func aggregateStatisticsReducer(inputChan, outputChan chan *store.Record) {
	var nodeId []byte
	grouper := transformer.GroupRecords(inputChan, &nodeId)
	for grouper.NextGroup() {
		aggregateStatistics := newAggregateStatistics()
		for grouper.NextRecord() {
			record := grouper.Read()
			var statistics AggregateStatistics
			if err := proto.Unmarshal(record.Value, &statistics); err != nil {
				panic(err)
			}
			*aggregateStatistics.Traces += *statistics.Traces
			*aggregateStatistics.Packets += *statistics.Packets
			*aggregateStatistics.PacketSeriesDropped += *statistics.PacketSeriesDropped
			*aggregateStatistics.PcapDropped += *statistics.PcapDropped
			*aggregateStatistics.InterfaceDropped += *statistics.InterfaceDropped
			*aggregateStatistics.Flows += *statistics.Flows
			*aggregateStatistics.DroppedFlows += *statistics.DroppedFlows
			*aggregateStatistics.Bytes += *statistics.Bytes
		}
		encodedStatistics, err := proto.Marshal(aggregateStatistics)
		if err != nil {
			panic(err)
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(nodeId),
			Value: encodedStatistics,
		}
	}
}

type aggregateStatisticsJsonStore struct {
	writer io.Writer
	first  bool
}

func (store *aggregateStatisticsJsonStore) BeginWriting() error {
	if _, err := fmt.Fprintf(store.writer, "["); err != nil {
		return err
	}
	store.first = true
	return nil
}

func (store *aggregateStatisticsJsonStore) WriteRecord(record *store.Record) error {
	var nodeId string
	lex.DecodeOrDie(record.Key, &nodeId)
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
	if _, err := fmt.Fprintf(store.writer, "[%q,%d,%d,%d,%d,%d,%d]", nodeId, *statistics.Traces, *statistics.Packets, *statistics.PacketSeriesDropped+*statistics.PcapDropped+*statistics.InterfaceDropped, *statistics.Flows, *statistics.DroppedFlows, *statistics.Bytes); err != nil {
		return err
	}
	return nil
}

func (store *aggregateStatisticsJsonStore) EndWriting() error {
	if _, err := fmt.Fprintf(store.writer, "]"); err != nil {
		return err
	}
	return nil
}
