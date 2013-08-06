package passive

import (
	"bytes"
	"fmt"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func runAvailabilityPipeline(startTimestamp int64, timestamps map[string]int64) {
	levelDbManager := store.NewSliceManager()

	tracesStore := levelDbManager.Writer("traces")
	tracesStore.BeginWriting()
	for encodedKey, timestamp := range timestamps {
		trace := Trace{
			TraceCreationTimestamp: proto.Int64(timestamp),
		}
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		tracesStore.WriteRecord(&store.Record{Key: []byte(encodedKey), Value: encodedTrace})
	}
	tracesStore.EndWriting()

	writer := bytes.NewBuffer([]byte{})
	transformer.RunPipeline(AvailabilityPipeline(levelDbManager, writer, startTimestamp, 1))
	fmt.Printf("%s", writer.Bytes())
}

func runAvailabilityPipelineAugmented(startTimestamp int64, timestamps map[string]int64, moreTimestamps map[string]int64) {
	levelDbManager := store.NewSliceManager()

	tracesStore := levelDbManager.Writer("traces")
	tracesStore.BeginWriting()
	for encodedKey, timestamp := range timestamps {
		trace := Trace{
			TraceCreationTimestamp: proto.Int64(timestamp),
		}
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		tracesStore.WriteRecord(&store.Record{Key: []byte(encodedKey), Value: encodedTrace})
	}
	tracesStore.EndWriting()

	writer := bytes.NewBuffer([]byte{})
	transformer.RunPipeline(AvailabilityPipeline(levelDbManager, writer, startTimestamp, 1))

	tracesStore.BeginWriting()
	for encodedKey, timestamp := range moreTimestamps {
		trace := Trace{
			TraceCreationTimestamp: proto.Int64(timestamp),
		}
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		tracesStore.WriteRecord(&store.Record{Key: []byte(encodedKey), Value: encodedTrace})
	}
	tracesStore.EndWriting()

	anotherTracesSlice := make([]*store.Record, 0)
	for encodedKey, timestamp := range moreTimestamps {
		trace := Trace{
			TraceCreationTimestamp: proto.Int64(timestamp),
		}
		encodedTrace, err := proto.Marshal(&trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		anotherTracesSlice = append(anotherTracesSlice, &store.Record{Key: []byte(encodedKey), Value: encodedTrace})
	}

	anotherWriter := bytes.NewBuffer([]byte{})
	transformer.RunPipeline(AvailabilityPipeline(levelDbManager, anotherWriter, startTimestamp, 1))
	fmt.Printf("%s", anotherWriter.Bytes())
}

func ExampleAvailability_simple() {
	runAvailabilityPipeline(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(20),
	})
	// Output:
	// [{"node0": [[0],[20000],null,null]}, 123000]
}

func ExampleAvailability_multipleSessions() {
	runAvailabilityPipeline(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(0))): int64(20),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(30),
	})
	// Output:
	// [{"node0": [[0,20000],[10000,30000],null,null]}, 123000]
}

func ExampleAvailability_missingSequenceNumbers() {
	runAvailabilityPipeline(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(30),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(0))): int64(40),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(50),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(3))): int64(60),
	})
	// Output:
	// [{"node0": [[0,40000],[10000,50000],[30000,60000],[30000,60000]]}, 123000]
}

func ExampleAvailability_multipleNodes() {
	runAvailabilityPipeline(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node1", "anon0", int64(0), int32(0))): int64(40),
		string(lex.EncodeOrDie("node1", "anon0", int64(0), int32(1))): int64(50),
	})
	// Output:
	// [{"node0": [[0],[10000],null,null],"node1": [[40000],[50000],null,null]}, 123000]
}

func ExampleAvailability_multipleNodesMissingSequenceNumbers() {
	runAvailabilityPipeline(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(30),
		string(lex.EncodeOrDie("node1", "anon0", int64(0), int32(0))): int64(40),
		string(lex.EncodeOrDie("node1", "anon0", int64(0), int32(1))): int64(50),
		string(lex.EncodeOrDie("node1", "anon0", int64(0), int32(3))): int64(60),
	})
	// Output:
	// [{"node0": [[0],[10000],[30000],[30000]],"node1": [[40000],[50000],[60000],[60000]]}, 123000]
}

func ExampleAvailability_missingFirstSequenceNumber() {
	runAvailabilityPipeline(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(1))): int64(20),
		string(lex.EncodeOrDie("node0", "anon0", int64(1), int32(2))): int64(25),
		string(lex.EncodeOrDie("node0", "anon0", int64(2), int32(0))): int64(30),
		string(lex.EncodeOrDie("node0", "anon0", int64(2), int32(1))): int64(40),
	})
	// Output:
	// [{"node0": [[0,30000],[10000,40000],[20000],[25000]]}, 123000]
}

func ExampleAvailability_augment() {
	runAvailabilityPipelineAugmented(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(20),
	}, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(30),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(4))): int64(40),
	})
	// Output:
	// [{"node0": [[0],[40000],null,null]}, 123000]
}

func ExampleAvailability_augmentOutOfOrder() {
	runAvailabilityPipelineAugmented(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(20),
	}, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(2))): int64(30),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(4))): int64(40),
	})
	// Output:
	// [{"node0": [[0],[40000],null,null]}, 123000]
}

func ExampleAvailability_augmentMissing() {
	runAvailabilityPipelineAugmented(123, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(0))): int64(0),
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(1))): int64(10),
	}, map[string]int64{
		string(lex.EncodeOrDie("node0", "anon0", int64(0), int32(3))): int64(20),
	})
	// Output:
	// [{"node0": [[0],[10000],[20000],[20000]]}, 123000]
}
