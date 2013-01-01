package passive_test

import (
	. "bismark/passive"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
)

func runAvailiabilityMapper(inputRecords []*LevelDbRecord, inputTable string) {
	mapperOutput := runTransform(AvailabilityMapper, inputRecords, inputTable)
	for _, entry := range mapperOutput {
		fmt.Printf("%s: %s\n", entry.Key, entry.Value)
	}
}

func runAvailiability(inputRecords []*LevelDbRecord, inputTable string) {
	mapperOutput := runTransform(AvailabilityMapper, inputRecords, inputTable)
	reducerOutput := runTransform(AvailabilityReducer, mapperOutput, "availability_with_nonce")
	for _, entry := range reducerOutput {
		fmt.Printf("%s: %s\n", entry.Key, entry.Value)
	}
}

func makeTrace(timestamp int64) *Trace {
	return &Trace{
		TraceCreationTimestamp: proto.Int64(timestamp),
	}
}

func ExampleAvailabilityMapper_simple() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session0:2", makeTrace(20)),
	}
	runAvailiabilityMapper(records, "table")

	// Output:
	// availability_with_nonce:node0:00000000000000000000: 00000000000000000000,00000000000000000020
}

func ExampleAvailabilityMapper_multipleSessions() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session1:0", makeTrace(20)),
		createLevelDbRecord("table:node0:anon0:session1:1", makeTrace(30)),
	}
	runAvailiabilityMapper(records, "table")

	// Output:
	// availability_with_nonce:node0:00000000000000000000: 00000000000000000000,00000000000000000010
	// availability_with_nonce:node0:00000000000000000001: 00000000000000000020,00000000000000000030
}

func ExampleAvailabilityMapper_missingSequenceNumbers() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session0:3", makeTrace(30)),
		createLevelDbRecord("table:node0:anon0:session1:0", makeTrace(40)),
		createLevelDbRecord("table:node0:anon0:session1:1", makeTrace(50)),
		createLevelDbRecord("table:node0:anon0:session1:3", makeTrace(60)),
	}
	runAvailiabilityMapper(records, "table")

	// Output:
	// availability_with_nonce:node0:00000000000000000000: 00000000000000000000,00000000000000000010
	// availability_with_nonce:node0:00000000000000000001: 00000000000000000040,00000000000000000050
}

func ExampleAvailabilityMapper_multipleNodes() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node1:anon0:session0:0", makeTrace(40)),
		createLevelDbRecord("table:node1:anon0:session0:1", makeTrace(50)),
	}
	runAvailiabilityMapper(records, "table")

	// Output:
	// availability_with_nonce:node0:00000000000000000000: 00000000000000000000,00000000000000000010
	// availability_with_nonce:node1:00000000000000000001: 00000000000000000040,00000000000000000050
}

func ExampleAvailabilityMapper_multipleNodesMissingSequenceNumbers() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session0:3", makeTrace(30)),
		createLevelDbRecord("table:node1:anon0:session0:0", makeTrace(40)),
		createLevelDbRecord("table:node1:anon0:session0:1", makeTrace(50)),
		createLevelDbRecord("table:node1:anon0:session0:3", makeTrace(60)),
	}
	runAvailiabilityMapper(records, "table")

	// Output:
	// availability_with_nonce:node0:00000000000000000000: 00000000000000000000,00000000000000000010
	// availability_with_nonce:node1:00000000000000000001: 00000000000000000040,00000000000000000050
}

func ExampleAvailabilityMapper_missingFirstSequenceNumber() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session1:1", makeTrace(20)),
		createLevelDbRecord("table:node0:anon0:session1:2", makeTrace(25)),
		createLevelDbRecord("table:node0:anon0:session2:0", makeTrace(30)),
		createLevelDbRecord("table:node0:anon0:session2:1", makeTrace(40)),
	}
	runAvailiabilityMapper(records, "table")

	// Output:
	// availability_with_nonce:node0:00000000000000000000: 00000000000000000000,00000000000000000010
	// availability_with_nonce:node0:00000000000000000001: 00000000000000000030,00000000000000000040
}

func ExampleAvailability_simple() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session0:2", makeTrace(20)),
	}
	runAvailiability(records, "table")

	// Output:
	// availability:node0: [[0],[20000]]
}

func ExampleAvailability_multipleSessions() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session0:2", makeTrace(20)),
		createLevelDbRecord("table:node0:anon0:session1:0", makeTrace(30)),
		createLevelDbRecord("table:node0:anon0:session1:1", makeTrace(40)),
		createLevelDbRecord("table:node0:anon0:session1:2", makeTrace(50)),
	}
	runAvailiability(records, "table")

	// Output:
	// availability:node0: [[0,30000],[20000,50000]]
}

func ExampleAvailability_multipleNodes() {
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", makeTrace(0)),
		createLevelDbRecord("table:node0:anon0:session0:1", makeTrace(10)),
		createLevelDbRecord("table:node0:anon0:session0:2", makeTrace(20)),
		createLevelDbRecord("table:node1:anon0:session0:0", makeTrace(30)),
		createLevelDbRecord("table:node1:anon0:session0:1", makeTrace(40)),
		createLevelDbRecord("table:node1:anon0:session0:2", makeTrace(50)),
	}
	runAvailiability(records, "table")

	// Output:
	// availability:node0: [[0],[20000]]
	// availability:node1: [[30000],[50000]]
}
