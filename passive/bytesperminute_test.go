package passive_test

import (
	. "bismark/passive"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"time"
)

func makePacketSeriesEntry(timestamp int64, size int32) *PacketSeriesEntry {
	return &PacketSeriesEntry{
		TimestampMicroseconds: proto.Int64(timestamp),
		Size: proto.Int32(size),
	}
}

func runBytesPerMinute(inputRecords []*LevelDbRecord, inputTable string) {
	mapperOutput := runTransform(BytesPerMinuteMapper, inputRecords, inputTable)
	reducerOutput := runTransform(BytesPerMinuteReducer, mapperOutput, "bytes_per_minute_mapped")
	for _, entry := range reducerOutput {
		fmt.Printf("%s: %v\n", entry.Key, decodeInt64(entry.Value))
	}
}

func ExampleBytesPerMinute_simple() {
	trace1 := &Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace1.PacketSeries[0] = makePacketSeriesEntry(10, 20)
	trace1.PacketSeries[1] = makePacketSeriesEntry(30, 40)
	trace2 := &Trace{}
	trace2.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace2.PacketSeries[0] = makePacketSeriesEntry(50, 60)
	trace2.PacketSeries[1] = makePacketSeriesEntry(70, 80)
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:seq0", trace1),
		createLevelDbRecord("table:node0:anon0:session0:seq1", trace2),
	}
	runBytesPerMinute(records, "table")

	// Output:
	// bytes_per_minute:node0:00000000000000000000: 200
}

func ExampleBytesPerMinute_twoMinutes() {
	trace1 := &Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace1.PacketSeries[0] = makePacketSeriesEntry(10 * int64(time.Second / time.Microsecond), 20)
	trace1.PacketSeries[1] = makePacketSeriesEntry(30 * int64(time.Second / time.Microsecond), 40)
	trace2 := &Trace{}
	trace2.PacketSeries = make([]*PacketSeriesEntry, 2)
	trace2.PacketSeries[0] = makePacketSeriesEntry(50 * int64(time.Second / time.Microsecond), 60)
	trace2.PacketSeries[1] = makePacketSeriesEntry(70 * int64(time.Second / time.Microsecond), 80)
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:seq0", trace1),
		createLevelDbRecord("table:node0:anon0:session0:seq1", trace2),
	}
	runBytesPerMinute(records, "table")

	// Output:
	// bytes_per_minute:node0:00000000000000000000: 120
	// bytes_per_minute:node0:00000000000000000060: 80
}

func ExampleBytesPerMinute_multipleSessions() {
	trace1 := &Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 1)
	trace1.PacketSeries[0] = makePacketSeriesEntry(0, 20)
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:seq0", trace1),
		createLevelDbRecord("table:node0:anon0:session1:seq0", trace1),
		createLevelDbRecord("table:node0:anon1:session0:seq0", trace1),
	}
	runBytesPerMinute(records, "table")

	// Output:
	// bytes_per_minute:node0:00000000000000000000: 60
}

func ExampleBytesPerMinute_multipleNodes() {
	trace1 := &Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 1)
	trace1.PacketSeries[0] = makePacketSeriesEntry(0, 20)
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:seq0", trace1),
		createLevelDbRecord("table:node1:anon0:session0:seq0", trace1),
	}
	runBytesPerMinute(records, "table")

	// Output:
	// bytes_per_minute:node0:00000000000000000000: 20
	// bytes_per_minute:node1:00000000000000000000: 20
}
