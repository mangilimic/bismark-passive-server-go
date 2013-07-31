package passive

import (
	"fmt"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"github.com/sburnett/transformer/store"
)

func makePacketSeriesEntry(timestamp int64, size int32) *PacketSeriesEntry {
	return &PacketSeriesEntry{
		TimestampMicroseconds: proto.Int64(timestamp),
		Size: proto.Int32(size),
	}
}

func runBytesPerMinutePipeline(allTraces ...map[string]Trace) {
	bytesPerMinuteStore := store.SliceStore{}
	bytesPerHourStore := store.SliceStore{}
	bytesPerHourPostgresStore := store.SliceStore{}
	tracesStore := store.SliceStore{}
	mappedStore := store.SliceStore{}
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
		transformer.RunPipeline(BytesPerMinutePipeline(&tracesStore, &mappedStore, &bytesPerMinuteStore, &bytesPerHourStore, &bytesPerHourPostgresStore, &traceKeyRangesStore, &consolidatedTraceKeyRangesStore, 1), 0)
	}

	bytesPerMinuteStore.BeginReading()
	for {
		record, err := bytesPerMinuteStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		var nodeId string
		var timestamp, count int64
		key.DecodeOrDie(record.Key, &nodeId, &timestamp)
		key.DecodeOrDie(record.Value, &count)
		fmt.Printf("%s,%d: %d\n", nodeId, timestamp, count)
	}
	bytesPerMinuteStore.EndReading()
}

func ExampleBytesPerMinute_simple() {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace1.PacketSeries[0] = makePacketSeriesEntry(10, 20)
	trace1.PacketSeries[1] = makePacketSeriesEntry(30, 40)
	trace2 := Trace{}
	trace2.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace2.PacketSeries[0] = makePacketSeriesEntry(50, 60)
	trace2.PacketSeries[1] = makePacketSeriesEntry(70, 80)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(1))): trace2,
	}
	runBytesPerMinutePipeline(records)

	// Output:
	// node0,0: 200
}

func ExampleBytesPerMinute_twoMinutes() {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace1.PacketSeries[0] = makePacketSeriesEntry(10*int64(time.Second/time.Microsecond), 20)
	trace1.PacketSeries[1] = makePacketSeriesEntry(30*int64(time.Second/time.Microsecond), 40)
	trace2 := Trace{}
	trace2.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace2.PacketSeries[0] = makePacketSeriesEntry(50*int64(time.Second/time.Microsecond), 60)
	trace2.PacketSeries[1] = makePacketSeriesEntry(70*int64(time.Second/time.Microsecond), 80)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(1))): trace2,
	}
	runBytesPerMinutePipeline(records)

	// Output:
	// node0,0: 120
	// node0,60: 80
}

func ExampleBytesPerMinute_multipleSessions() {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 1)
	trace1.PacketSeries[0] = makePacketSeriesEntry(0, 20)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", "session1", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon1", "session0", int32(0))): trace1,
	}
	runBytesPerMinutePipeline(records)

	// Output:
	// node0,0: 60
}

func ExampleBytesPerMinute_multipleNodes() {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 1)
	trace1.PacketSeries[0] = makePacketSeriesEntry(0, 20)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node1", "anon0", "session0", int32(0))): trace1,
	}
	runBytesPerMinutePipeline(records)

	// Output:
	// node0,0: 20
	// node1,0: 20
}

func ExampleBytesPerMinute_multipleRuns() {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace1.PacketSeries[0] = makePacketSeriesEntry(10, 20)
	trace1.PacketSeries[1] = makePacketSeriesEntry(30, 40)
	trace2 := Trace{}
	trace2.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace2.PacketSeries[0] = makePacketSeriesEntry(50, 60)
	trace2.PacketSeries[1] = makePacketSeriesEntry(70, 80)
	trace3 := Trace{}
	trace3.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace3.PacketSeries[0] = makePacketSeriesEntry(10*int64(time.Second/time.Microsecond), 20)
	trace3.PacketSeries[1] = makePacketSeriesEntry(30*int64(time.Second/time.Microsecond), 40)
	trace4 := Trace{}
	trace4.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace4.PacketSeries[0] = makePacketSeriesEntry(50*int64(time.Second/time.Microsecond), 60)
	trace4.PacketSeries[1] = makePacketSeriesEntry(70*int64(time.Second/time.Microsecond), 80)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(2))): trace3,
	}
	moreRecords := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(1))): trace2,
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(3))): trace4,
	}
	runBytesPerMinutePipeline(records, moreRecords)

	// Output:
	// node0,0: 320
	// node0,60: 80
}
