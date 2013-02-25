package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
)

func runAvailabilityPipeline(startTimestamp int64, timestamps map[string]int64) {
	tracesSlice := make([]*transformer.LevelDbRecord, 0)
	for encodedKey, timestamp := range timestamps {
		trace := Trace{
			TraceCreationTimestamp: proto.Int64(timestamp),
		}
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		tracesSlice = append(tracesSlice, &transformer.LevelDbRecord{Key: []byte(encodedKey), Value: encodedTrace})
	}

	tracesStore := transformer.SliceStore(tracesSlice)
	intervalsStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	nodesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	writer := bytes.NewBuffer([]byte{})
	transformer.RunPipeline(AvailabilityPipeline(&tracesStore, &intervalsStore, &nodesStore, writer, startTimestamp, 1), 0)
	fmt.Printf("%s", writer.Bytes())
}

func ExampleAvailabilityMapper_simple() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(20),
	})
	// Output:
	// [{"node0": [[0],[20000]]}, 123000]
}

func ExampleAvailabilityMapper_multipleSessions() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(0))): int64(20),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(30),
	})
	// Output:
	// [{"node0": [[0,20000],[10000,30000]]}, 123000]
}

func ExampleAvailabilityMapper_missingSequenceNumbers() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(30),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(0))): int64(40),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(50),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(3))): int64(60),
	})
	// Output:
	// [{"node0": [[0,40000],[10000,50000]]}, 123000]
}

func ExampleAvailabilityMapper_multipleNodes() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(0))): int64(40),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(1))): int64(50),
	})
	// Output:
	// [{"node0": [[0],[10000]],"node1": [[40000],[50000]]}, 123000]
}

func ExampleAvailabilityMapper_multipleNodesMissingSequenceNumbers() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(30),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(0))): int64(40),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(1))): int64(50),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(3))): int64(60),
	})
	// Output:
	// [{"node0": [[0],[10000]],"node1": [[40000],[50000]]}, 123000]
}

func ExampleAvailabilityMapper_missingFirstSequenceNumber() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(20),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(2))): int64(25),
		string(key.EncodeOrDie("node0", "anon0", int64(2), int32(0))): int64(30),
		string(key.EncodeOrDie("node0", "anon0", int64(2), int32(1))): int64(40),
	})
	// Output:
	// [{"node0": [[0,30000],[10000,40000]]}, 123000]
}
