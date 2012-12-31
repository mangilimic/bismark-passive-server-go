package passive_test

import (
	. "bismark/passive"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
)

func runBytesPerDevice(inputRecords []*LevelDbRecord, inputTable string) {
	mapFromTraceOutput := runTransform(MapFromTrace, inputRecords, inputTable)
	//fmt.Printf("mapFromTraceOutput:\n")
	//for _, entry := range mapFromTraceOutput {
	//	fmt.Printf("%s: %v / %s\n", entry.Key, decodeInt64(entry.Value), entry.Value)
	//}
	joinMacAndFlowIdOutput := runTransform(JoinMacAndFlowId, mapFromTraceOutput, "ip_to_mac_and_flow")
	//fmt.Printf("joinMacAndFlowIdOutput:\n")
	//for _, entry := range joinMacAndFlowIdOutput {
	//	fmt.Printf("%s: %v / %s\n", entry.Key, decodeInt64(entry.Value), entry.Value)
	//}
	joinMacAndTimestampOutput := runTransform(JoinMacAndTimestamp, mergeOutputs(mapFromTraceOutput, joinMacAndFlowIdOutput) , "flow_to_bytes_and_mac")
	//fmt.Printf("joinMacAndTimestampOutput:\n")
	//for _, entry := range joinMacAndTimestampOutput {
	//	fmt.Printf("%s: %v / %s\n", entry.Key, decodeInt64(entry.Value), entry.Value)
	//}
	reducerOutput := runTransform(BytesPerDeviceReduce, joinMacAndTimestampOutput, "bytes_per_device_with_nonce")
	//fmt.Printf("output:\n")
	for _, entry := range reducerOutput {
		fmt.Printf("%s: %v\n", entry.Key, decodeInt64(entry.Value))
	}
}

func ExampleBytesPerDevice_single() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 10
}

func ExampleBytesPerDevice_missingMac() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 10
}

func ExampleBytesPerDevice_missingFlow() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	fmt.Printf("No output")

	// Output:
	// No output
}

func ExampleBytesPerDevice_roundToMinute() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1e6 * 61),  // 61 seconds past midnight on January 1, 1970
				Size: proto.Int32(20),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000060: 20
}

func ExampleBytesPerDevice_multipleMinutes() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1e6 * 60),  // 1 minute past midnight on January 1, 1970
				Size: proto.Int32(20),
				FlowId: proto.Int32(4),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1e6 * 120),  // 2 minutes past midnight on January 1, 1970
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000060: 20
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000120: 10
}

func ExampleBytesPerDevice_multipleFlows() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(20),
				FlowId: proto.Int32(5),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
			&FlowTableEntry{
				FlowId: proto.Int32(5),
				SourceIp: proto.String("4.3.2.1"),
				DestinationIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 30
}

func ExampleBytesPerDevice_twoDevicesPerFlow() {
	trace := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(20),
				FlowId: proto.Int32(5),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
			&FlowTableEntry{
				FlowId: proto.Int32(5),
				SourceIp: proto.String("4.3.2.1"),
				DestinationIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
			&AddressTableEntry{
				IpAddress: proto.String("4.3.2.1"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 30
	// bytes_per_device:node0:FFEEDDCCBBAA:00000000000000000000: 30
}

func ExampleBytesPerDevice_maskFlows() {
	trace1 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(30),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.1.1.1"),
				DestinationIp: proto.String("4.4.4.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
			&AddressTableEntry{
				IpAddress: proto.String("1.1.1.1"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	trace2 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
	}
	trace3 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(2),
				FlowId: proto.Int32(4),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace1),
		createLevelDbRecord("table:node0:anon0:session0:1", trace2),
		createLevelDbRecord("table:node0:anon0:session0:2", trace3),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 12
	// bytes_per_device:node0:FFEEDDCCBBAA:00000000000000000000: 30
}

func ExampleBytesPerDevice_macBoundAtStartOfFlow() {
	trace1 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(12),
				FlowId: proto.Int32(4),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace1),
		createLevelDbRecord("table:node0:anon0:session0:1", trace2),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 22
}

func ExampleBytesPerDevice_maskMac() {
	trace1 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(12),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace1),
		createLevelDbRecord("table:node0:anon0:session0:1", trace2),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 10
	// bytes_per_device:node0:FFEEDDCCBBAA:00000000000000000000: 12
}

func ExampleBytesPerDevice_multipleNodes() {
	trace1 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(10),
				FlowId: proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := &Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size: proto.Int32(20),
				FlowId: proto.Int32(3),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId: proto.Int32(3),
				SourceIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry {
			&AddressTableEntry{
				IpAddress: proto.String("4.3.2.1"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	records := []*LevelDbRecord{
		createLevelDbRecord("table:node0:anon0:session0:0", trace1),
		createLevelDbRecord("table:node1:anon0:session0:2", trace2),
	}
	runBytesPerDevice(records, "table")

	// Output:
	// bytes_per_device:node0:AABBCCDDEEFF:00000000000000000000: 10
	// bytes_per_device:node1:FFEEDDCCBBAA:00000000000000000000: 20
}
