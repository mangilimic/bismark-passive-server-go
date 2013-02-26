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
	consolidatedStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	nodesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	writer := bytes.NewBuffer([]byte{})
	excludeRangesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	transformer.RunPipeline(AvailabilityPipeline(&tracesStore, &intervalsStore, &consolidatedStore, &nodesStore, writer, &excludeRangesStore, startTimestamp, 1), 0)
	fmt.Printf("%s", writer.Bytes())
}

func runAvailabilityPipelineAugmented(startTimestamp int64, timestamps map[string]int64, moreTimestamps map[string]int64) {
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
	consolidatedStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	nodesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	writer := bytes.NewBuffer([]byte{})
	excludeRangesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	transformer.RunPipeline(AvailabilityPipeline(&tracesStore, &intervalsStore, &consolidatedStore, &nodesStore, writer, &excludeRangesStore, startTimestamp, 1), 0)

	anotherTracesSlice := make([]*transformer.LevelDbRecord, 0)
	for encodedKey, timestamp := range moreTimestamps {
		trace := Trace{
			TraceCreationTimestamp: proto.Int64(timestamp),
		}
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		anotherTracesSlice = append(anotherTracesSlice, &transformer.LevelDbRecord{Key: []byte(encodedKey), Value: encodedTrace})
	}

	anotherTracesStore := transformer.SliceStore(anotherTracesSlice)
	anotherConsolidatedStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	anotherNodesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	anotherWriter := bytes.NewBuffer([]byte{})
	anotherExcludeRangesStore := transformer.SliceStore(make([]*transformer.LevelDbRecord, 0))
	transformer.RunPipeline(AvailabilityPipeline(&anotherTracesStore, &intervalsStore, &anotherConsolidatedStore, &anotherNodesStore, anotherWriter, &anotherExcludeRangesStore, startTimestamp, 1), 0)

	fmt.Printf("%s", anotherWriter.Bytes())
}

func ExampleAvailability_simple() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(20),
	})
	// Output:
	// [{"node0": [[0],[20000]]}, 123000]
}

func ExampleAvailability_multipleSessions() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(0))): int64(20),
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(30),
	})
	// Output:
	// [{"node0": [[0,20000],[10000,30000]]}, 123000]
}

func ExampleAvailability_missingSequenceNumbers() {
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

func ExampleAvailability_multipleNodes() {
	runAvailabilityPipeline(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(0))): int64(40),
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(1))): int64(50),
	})
	// Output:
	// [{"node0": [[0],[10000]],"node1": [[40000],[50000]]}, 123000]
}

func ExampleAvailability_multipleNodesMissingSequenceNumbers() {
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

func ExampleAvailability_missingFirstSequenceNumber() {
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

func ExampleAvailability_augment() {
	runAvailabilityPipelineAugmented(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(20),
	}, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(30),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(4))): int64(40),
	})
	// Output:
	// [{"node0": [[0],[40000]]}, 123000]
}

func ExampleAvailability_augmentOutOfOrder() {
	runAvailabilityPipelineAugmented(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(20),
	}, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(30),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(4))): int64(40),
	})
	// Output:
	// [{"node0": [[0],[40000]]}, 123000]
}

func ExampleAvailability_augmentMissing() {
	runAvailabilityPipelineAugmented(123, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
	}, map[string]int64{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(20),
	})
	// Output:
	// [{"node0": [[0],[10000]]}, 123000]
}
