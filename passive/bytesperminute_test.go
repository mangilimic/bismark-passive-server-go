package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"sort"
	"testing"
	"time"
)

func makePacketSeriesEntry(timestamp int64, size int32) *PacketSeriesEntry {
	return &PacketSeriesEntry{
		TimestampMicroseconds: proto.Int64(timestamp),
		Size:                  proto.Int32(size),
	}
}

func runBytesPerMinutePipeline(t *testing.T, traces map[string]Trace, expectedOutput []transformer.LevelDbRecord) {
	databases := make(map[string]map[string]string)
	traceDatabase := make(map[string]string)
	for encodedKey, trace := range traces {
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			t.Fatalf("Error encoding protocol buffer: %v", err)
		}
		traceDatabase[encodedKey] = string(encodedTrace)
	}
	databases["traces"] = traceDatabase

	transformer.RunPipelineWithoutLevelDb(BytesPerMinutePipeline(1), databases, 100)
	database, ok := databases["bytesperminute"]
	if !ok {
		t.Fatalf("Missing expected output database")
	}
	actualOutput := make([]*transformer.LevelDbRecord, 0)
	for k, v := range database {
		actualOutput = append(actualOutput, &transformer.LevelDbRecord{Key: []byte(k), Value: []byte(v)})
	}
	sort.Sort(transformer.LevelDbRecordSlice(actualOutput))
	for idx, expectedRecord := range expectedOutput {
		if len(actualOutput) <= idx {
			t.Fatalf("Missing expected record %s: %s", expectedRecord.Key, expectedRecord.Value)
		}
		actualRecord := actualOutput[idx]
		if !bytes.Equal(expectedRecord.Key, actualRecord.Key) {
			t.Fatalf("Expected key: %s, Got: %s", expectedRecord.Key, actualRecord.Key)
		}
		if !bytes.Equal(expectedRecord.Value, actualRecord.Value) {
			t.Fatalf("Expected value: %s, Got: %s", expectedRecord.Value, actualRecord.Value)
		}
	}
	for idx := len(expectedOutput); idx < len(actualOutput); idx++ {
		t.Fatalf("Got extra record %s: %s", actualOutput[idx].Key, actualOutput[idx].Value)
	}
}

func TestBytesPerMinute_simple(t *testing.T) {
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
	expectedOutput := []transformer.LevelDbRecord{
		transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", int64(0)),
			Value: key.EncodeOrDie(int64(200)),
		},
	}
	runBytesPerMinutePipeline(t, records, expectedOutput)
}

func TestBytesPerMinute_twoMinutes(t *testing.T) {
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
	expectedOutput := []transformer.LevelDbRecord{
		transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", int64(0)),
			Value: key.EncodeOrDie(int64(120)),
		},
		transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", int64(60)),
			Value: key.EncodeOrDie(int64(80)),
		},
	}
	runBytesPerMinutePipeline(t, records, expectedOutput)
}

func TestBytesPerMinute_multipleSessions(t *testing.T) {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 1)
	trace1.PacketSeries[0] = makePacketSeriesEntry(0, 20)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", "session1", int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon1", "session0", int32(0))): trace1,
	}
	expectedOutput := []transformer.LevelDbRecord{
		transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", int64(0)),
			Value: key.EncodeOrDie(int64(60)),
		},
	}
	runBytesPerMinutePipeline(t, records, expectedOutput)
}

func TestBytesPerMinute_multipleNodes(t *testing.T) {
	trace1 := Trace{}
	trace1.PacketSeries = make([]*PacketSeriesEntry, 1)
	trace1.PacketSeries[0] = makePacketSeriesEntry(0, 20)
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", "session0", int32(0))): trace1,
		string(key.EncodeOrDie("node1", "anon0", "session0", int32(0))): trace1,
	}
	expectedOutput := []transformer.LevelDbRecord{
		transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", int64(0)),
			Value: key.EncodeOrDie(int64(20)),
		},
		transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node1", int64(0)),
			Value: key.EncodeOrDie(int64(20)),
		},
	}
	runBytesPerMinutePipeline(t, records, expectedOutput)
}
