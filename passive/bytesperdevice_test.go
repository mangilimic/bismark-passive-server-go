package passive

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
)

func runBytesPerDevicePipeline(consistentRanges []*transformer.LevelDbRecord, allTraces ...map[string]Trace) {
	tracesStore := transformer.SliceStore{}
	availabilityIntervalsStore := transformer.SliceStore{}
	availabilityIntervalsStore.BeginWriting()
	for _, record := range consistentRanges {
		availabilityIntervalsStore.WriteRecord(record)
	}
	availabilityIntervalsStore.EndWriting()
	addressTableStore := transformer.SliceStore{}
	flowTableStore := transformer.SliceStore{}
	packetsStore := transformer.SliceStore{}
	flowIdToMacStore := transformer.SliceStore{}
	flowIdToMacsStore := transformer.SliceStore{}
	bytesPerDeviceUnreducedStore := transformer.SliceStore{}
	bytesPerDeviceStore := transformer.SliceStore{}
	traceKeyRangesStore := transformer.SliceStore{}
	consolidatedTraceKeyRangesStore := transformer.SliceStore{}

	for _, traces := range allTraces {
		tracesStore.BeginWriting()
		for encodedKey, trace := range traces {
			encodedTrace, err := proto.Marshal(&trace)
			if err != nil {
				panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
			}
			tracesStore.WriteRecord(&transformer.LevelDbRecord{Key: []byte(encodedKey), Value: encodedTrace})
		}
		tracesStore.EndWriting()

		transformer.RunPipeline(BytesPerDevicePipeline(&tracesStore, &availabilityIntervalsStore, &addressTableStore, &flowTableStore, &packetsStore, &flowIdToMacStore, &flowIdToMacsStore, &bytesPerDeviceUnreducedStore, &bytesPerDeviceStore, &traceKeyRangesStore, &consolidatedTraceKeyRangesStore, 1), 0)
	}

	bytesPerDeviceStore.BeginReading()
	for {
		record, err := bytesPerDeviceStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		var nodeId, macAddress string
		var timestamp, count int64
		key.DecodeOrDie(record.Key, &nodeId, &macAddress, &timestamp)
		key.DecodeOrDie(record.Value, &count)
		fmt.Printf("%s,%s,%d: %d\n", nodeId, macAddress, timestamp, count)
	}
	bytesPerDeviceStore.EndReading()
}

func ExampleBytesPerDevice_single() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:   proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 10
}

func ExampleBytesPerDevice_missingSequenceNumber() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:   proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(13),
				FlowId:                proto.Int32(4),
			},
		},
	}
	trace3 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node1", "anon1", int64(2), int32(0)),
			Value: key.EncodeOrDie("node1", "anon1", int64(2), int32(2)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(3))): trace3,
		string(key.EncodeOrDie("node1", "anon1", int64(2), int32(0))): trace1,
		string(key.EncodeOrDie("node1", "anon1", int64(2), int32(1))): trace2,
		string(key.EncodeOrDie("node1", "anon1", int64(2), int32(2))): trace3,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 23
	// node1,AABBCCDDEEFF,0: 43
}

func ExampleBytesPerDevice_missingMac() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 10
}

func ExampleBytesPerDevice_missingFlow() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	fmt.Printf("No output")

	// Output:
	// No output
}

func ExampleBytesPerDevice_roundToMinute() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1e6 * 61), // 61 seconds past midnight on January 1, 1970
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,60: 20
}

func ExampleBytesPerDevice_multipleMinutes() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1e6 * 60), // 1 minute past midnight on January 1, 1970
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(4),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1e6 * 120), // 2 minutes past midnight on January 1, 1970
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,60: 20
	// node0,AABBCCDDEEFF,120: 10
}

func ExampleBytesPerDevice_multipleFlows() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(5),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(5),
				SourceIp:      proto.String("4.3.2.1"),
				DestinationIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 30
}

func ExampleBytesPerDevice_twoMacsPerFlow() {
	trace := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(5),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(5),
				SourceIp:      proto.String("4.3.2.1"),
				DestinationIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
			&AddressTableEntry{
				IpAddress:  proto.String("4.3.2.1"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 30
	// node0,FFEEDDCCBBAA,0: 30
}

func ExampleBytesPerDevice_maskFlows() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(30),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.1.1.1"),
				DestinationIp: proto.String("4.4.4.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
			&AddressTableEntry{
				IpAddress:  proto.String("1.1.1.1"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
	}
	trace3 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(2),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(2)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): trace3,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 12
	// node0,FFEEDDCCBBAA,0: 30
}

func ExampleBytesPerDevice_maskMacAndFlows() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(30),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.4.4.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.4.4.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 30
	// node0,FFEEDDCCBBAA,0: 20
}

func ExampleBytesPerDevice_macBoundAtStartOfFlow() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(12),
				FlowId:                proto.Int32(4),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 22
}

func ExampleBytesPerDevice_maskMac() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(12),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("1.2.3.4"),
				DestinationIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 10
	// node0,FFEEDDCCBBAA,0: 12
}

func ExampleBytesPerDevice_multipleNodes() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:   proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(3),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:   proto.Int32(3),
				SourceIp: proto.String("4.3.2.1"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("4.3.2.1"),
				MacAddress: proto.String("FFEEDDCCBBAA"),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node1", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node1", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(0))): trace2,
	}
	runBytesPerDevicePipeline(consistentRanges, records)

	// Output:
	// node0,AABBCCDDEEFF,0: 10
	// node1,FFEEDDCCBBAA,0: 20
}

func ExampleBytesPerDevice_multipleSessions() {
	trace1 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(4),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:   proto.Int32(4),
				SourceIp: proto.String("1.2.3.4"),
			},
		},
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				IpAddress:  proto.String("1.2.3.4"),
				MacAddress: proto.String("AABBCCDDEEFF"),
			},
		},
	}
	trace2 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(13),
				FlowId:                proto.Int32(4),
			},
		},
	}
	trace3 := Trace{
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(20),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(2)),
		},
	}
	firstRecords := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
	}
	secondRecords := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(2))): trace3,
	}
	runBytesPerDevicePipeline(consistentRanges, firstRecords, secondRecords)

	// Output:
	// node0,AABBCCDDEEFF,0: 43
}
