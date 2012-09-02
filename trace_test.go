package bismarkpassive

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"testing"
)

func printSections(sections [][]string) {
	fmt.Println("{")
	for _, section := range sections {
		fmt.Println(" {")
		for _, line := range section {
			fmt.Printf("  %s\n", line)
		}
		fmt.Println(" }")
	}
	fmt.Println("}")
}

func ExampleLinesToSections_simple() {
	lines := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte(""),
		[]byte("test"),
	}
	printSections(linesToSections(lines))

	// Output:
	// {
	//  {
	//   hello
	//   world
	//  }
	//  {
	//   test
	//  }
	// }
}

func ExampleLinesToSections_empty() {
	lines := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte(""),
		[]byte(""),
		[]byte("test"),
	}
	printSections(linesToSections(lines))

	// Output:
	// {
	//  {
	//   hello
	//   world
	//  }
	//  {
	//  }
	//  {
	//   test
	//  }
	// }
}

func ExampleLinesToSections_trim() {
	lines := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte(""),
	}
	printSections(linesToSections(lines))

	// Output:
	// {
	//  {
	//   hello
	//   world
	//  }
	// }
}

func checkForSectionError(t *testing.T, parseSection func([]string, *Trace) error, lines []string) {
	trace := Trace{}
	err := parseSection(lines, &trace)
	if err == nil {
		t.Fatalf("Trace should be invalid. Instead got trace: %s", trace)
	}
	if e, ok := err.(*sectionError); !ok {
		t.Fatalf("Should return sectionError. Instead got error: %s", e)
	}
}

func checkProtosEqual(t *testing.T, expectedTrace *Trace, trace *Trace) {
	if !proto.Equal(expectedTrace, trace) {
		t.Fatalf("Protocol buffers not equal:\nExpected: %s\nActual:   %s", expectedTrace, trace)
	}
}

func TestParseSectionIntro_Invalid(t *testing.T) {
	// Incompleteness
	checkForSectionError(t, parseSectionIntro, []string{})
	checkForSectionError(t, parseSectionIntro, []string{""})
	checkForSectionError(t, parseSectionIntro, []string{"10"})
	checkForSectionError(t, parseSectionIntro, []string{"10", ""})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", ""})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", ""})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76 85"})

	// Integer conversion
	checkForSectionError(t, parseSectionIntro, []string{"STR", "BUILDID", "NODEID 123 321 789", "76 85 99"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID STR 321 789", "76 85 99"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 STR 789", "76 85 99"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 STR", "76 85 99"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "STR 85 99"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76 STR 99"})
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76 85 STR"})
}

func TestParseSectionIntro_Valid(t *testing.T) {
	lines := []string{
		"10",
		"BUILDID",
		"NODEID 123 321 789",
		"12 23 34",
	}
	trace := Trace{}
	err := parseSectionIntro(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		FileFormatVersion: proto.Int32(10),
		BuildId:           proto.String("BUILDID"),
		NodeId:            proto.String("NODEID"),
		ProcessStartTimeMicroseconds: proto.Int64(123),
		SequenceNumber:               proto.Int32(321),
		TraceCreationTimestamp:       proto.Int64(789),
		PcapReceived:                 proto.Uint32(12),
		PcapDropped:                  proto.Uint32(23),
		InterfaceDropped:             proto.Uint32(34),
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionIntro_ValidOptional(t *testing.T) {
	lines := []string{
		"10",
		"BUILDID",
		"NODEID 123 321 789",
	}
	trace := Trace{}
	err := parseSectionIntro(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		FileFormatVersion: proto.Int32(10),
		BuildId:           proto.String("BUILDID"),
		NodeId:            proto.String("NODEID"),
		ProcessStartTimeMicroseconds: proto.Int64(123),
		SequenceNumber:               proto.Int32(321),
		TraceCreationTimestamp:       proto.Int64(789),
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionWhitelist_HasEntries(t *testing.T) {
	lines := []string{
		"first",
		"second",
	}
	trace := Trace{}
	err := parseSectionWhitelist(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		Whitelist: []string{
			"first",
			"second",
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionWhitelist_Empty(t *testing.T) {
	lines := []string{}
	trace := Trace{}
	err := parseSectionWhitelist(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		Whitelist: []string{},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionAnonymization_Anonymized(t *testing.T) {
	lines := []string{
		"signature",
	}
	trace := Trace{}
	err := parseSectionAnonymization(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		AnonymizationSignature: proto.String("signature"),
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionAnonymization_Unanonymized(t *testing.T) {
	lines := []string{
		"UNANONYMIZED",
	}
	trace := Trace{}
	err := parseSectionAnonymization(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		AnonymizationSignature: nil,
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionAnonymization_Missing(t *testing.T) {
	lines := []string{}
	trace := Trace{}
	err := parseSectionAnonymization(lines, &trace)
	if err == nil {
		t.Fatal("Expected error")
	}
}

func TestParseSectionPacketSeries_Valid(t *testing.T) {
	lines := []string{
		"10 11",
		"0 10 23",
		"20 12 45",
		"10 10 20",
	}
	trace := Trace{}
	err := parseSectionPacketSeries(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		PacketSeriesDropped: proto.Uint32(11),
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(10),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(23),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(30),
				Size:                  proto.Int32(12),
				FlowId:                proto.Int32(45),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(40),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(20),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionPacketSeries_Invalid(t *testing.T) {
	checkForSectionError(t, parseSectionPacketSeries, []string{})
	checkForSectionError(t, parseSectionPacketSeries, []string{""})
	checkForSectionError(t, parseSectionPacketSeries, []string{"10"})
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0"})
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10"})
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10 23", ""})
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10 23", "20"})
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10 23", "20 12"})
}

func TestParseSectionFlowTable_Valid(t *testing.T) {
	lines := []string{
		"10 11 12 13",
		"14 0 IP1 0 IP2 15 16 17",
		"18 1 IP3 1 IP4 19 20 21",
		"22 0 IP5 1 IP6 23 24 25",
		"26 1 IP7 0 IP8 27 28 29",
	}
	trace := Trace{}
	err := parseSectionFlowTable(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		FlowTableBaseline: proto.Int64(10),
		FlowTableSize:     proto.Uint32(11),
		FlowTableExpired:  proto.Int32(12),
		FlowTableDropped:  proto.Int32(13),
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:                  proto.Int32(14),
				SourceIpAnonymized:      proto.Bool(false),
				SourceIp:                proto.String("IP1"),
				DestinationIpAnonymized: proto.Bool(false),
				DestinationIp:           proto.String("IP2"),
				TransportProtocol:       proto.Int32(15),
				SourcePort:              proto.Int32(16),
				DestinationPort:         proto.Int32(17),
			},
			&FlowTableEntry{
				FlowId:                  proto.Int32(18),
				SourceIpAnonymized:      proto.Bool(true),
				SourceIp:                proto.String("IP3"),
				DestinationIpAnonymized: proto.Bool(true),
				DestinationIp:           proto.String("IP4"),
				TransportProtocol:       proto.Int32(19),
				SourcePort:              proto.Int32(20),
				DestinationPort:         proto.Int32(21),
			},
			&FlowTableEntry{
				FlowId:                  proto.Int32(22),
				SourceIpAnonymized:      proto.Bool(false),
				SourceIp:                proto.String("IP5"),
				DestinationIpAnonymized: proto.Bool(true),
				DestinationIp:           proto.String("IP6"),
				TransportProtocol:       proto.Int32(23),
				SourcePort:              proto.Int32(24),
				DestinationPort:         proto.Int32(25),
			},
			&FlowTableEntry{
				FlowId:                  proto.Int32(26),
				SourceIpAnonymized:      proto.Bool(true),
				SourceIp:                proto.String("IP7"),
				DestinationIpAnonymized: proto.Bool(false),
				DestinationIp:           proto.String("IP8"),
				TransportProtocol:       proto.Int32(27),
				SourcePort:              proto.Int32(28),
				DestinationPort:         proto.Int32(29),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionFlowTable_Invalid(t *testing.T) {
	checkForSectionError(t, parseSectionFlowTable, []string{})
	checkForSectionError(t, parseSectionFlowTable, []string{""})
	checkForSectionError(t, parseSectionFlowTable, []string{"10"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", ""})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 16"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 16 17", ""})

	checkForSectionError(t, parseSectionFlowTable, []string{"XX 11 12 13", "14 0 IP1 0 IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 XX 12 13", "14 0 IP1 0 IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 XX 13", "14 0 IP1 0 IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 XX", "14 0 IP1 0 IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "XX 0 IP1 0 IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 X IP1 0 IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 X IP2 15 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 XX 16 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 XX 17"})
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 16 XX"})
}

func TestParseSectionDnsTableA_Valid(t *testing.T) {
	lines := []string{
		"10 11",
		"12 13 0 DOM1 IP1 14",
		"15 16 1 DOM2 IP2 17",
	}
	trace := Trace{}
	err := parseSectionDnsTableA(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		ARecordsDropped:     proto.Int32(10),
		CnameRecordsDropped: proto.Int32(11),
		ARecord: []*DnsARecord{
			&DnsARecord{
				PacketId:   proto.Int32(12),
				AddressId:  proto.Int32(13),
				Anonymized: proto.Bool(false),
				Domain:     proto.String("DOM1"),
				IpAddress:  proto.String("IP1"),
				Ttl:        proto.Int32(14),
			},
			&DnsARecord{
				PacketId:   proto.Int32(15),
				AddressId:  proto.Int32(16),
				Anonymized: proto.Bool(true),
				Domain:     proto.String("DOM2"),
				IpAddress:  proto.String("IP2"),
				Ttl:        proto.Int32(17),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionDnsTableA_Invalid(t *testing.T) {
	checkForSectionError(t, parseSectionDnsTableA, []string{})
	checkForSectionError(t, parseSectionDnsTableA, []string{""})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", ""})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1 IP1"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1 IP1 14", ""})

	checkForSectionError(t, parseSectionDnsTableA, []string{"XX 11", "12 13 0 DOM1 IP1 14"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 XX", "12 13 0 DOM1 IP1 14"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "XX 13 0 DOM1 IP1 14"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 XX 0 DOM1 IP1 14"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 X DOM1 IP1 14"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 2 DOM1 IP1 14"})
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1 IP1 XX"})
}

func TestParseSectionDnsTableCname_Valid(t *testing.T) {
	lines := []string{
		"12 13 0 DOM1 0 CN1 14",
		"15 16 0 DOM2 1 CN2 17",
		"18 19 1 DOM3 0 CN3 20",
		"21 22 1 DOM4 1 CN4 23",
	}
	trace := Trace{}
	err := parseSectionDnsTableCname(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				PacketId:         proto.Int32(12),
				AddressId:        proto.Int32(13),
				DomainAnonymized: proto.Bool(false),
				Domain:           proto.String("DOM1"),
				CnameAnonymized:  proto.Bool(false),
				Cname:            proto.String("CN1"),
				Ttl:              proto.Int32(14),
			},
			&DnsCnameRecord{
				PacketId:         proto.Int32(15),
				AddressId:        proto.Int32(16),
				DomainAnonymized: proto.Bool(false),
				Domain:           proto.String("DOM2"),
				CnameAnonymized:  proto.Bool(true),
				Cname:            proto.String("CN2"),
				Ttl:              proto.Int32(17),
			},
			&DnsCnameRecord{
				PacketId:         proto.Int32(18),
				AddressId:        proto.Int32(19),
				DomainAnonymized: proto.Bool(true),
				Domain:           proto.String("DOM3"),
				CnameAnonymized:  proto.Bool(false),
				Cname:            proto.String("CN3"),
				Ttl:              proto.Int32(20),
			},
			&DnsCnameRecord{
				PacketId:         proto.Int32(21),
				AddressId:        proto.Int32(22),
				DomainAnonymized: proto.Bool(true),
				Domain:           proto.String("DOM4"),
				CnameAnonymized:  proto.Bool(true),
				Cname:            proto.String("CN4"),
				Ttl:              proto.Int32(23),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionDnsTableCname_Invalid(t *testing.T) {
	checkForSectionError(t, parseSectionDnsTableCname, []string{""})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12"})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13"})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0"})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1"})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1 CN1"})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1 CN1 14"})
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1 CN1 14", ""})
}

func TestParseSectionAddressTable_Valid(t *testing.T) {
	lines := []string{
		"10 11",
		"MAC1 IP1",
		"MAC2 IP2",
	}
	trace := Trace{}
	err := parseSectionAddressTable(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		AddressTableFirstId: proto.Int32(10),
		AddressTableSize:    proto.Int32(11),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("MAC1"),
				IpAddress:  proto.String("IP1"),
			},
			&AddressTableEntry{
				MacAddress: proto.String("MAC2"),
				IpAddress:  proto.String("IP2"),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionAddressTable_Invalid(t *testing.T) {
	checkForSectionError(t, parseSectionAddressTable, []string{})
	checkForSectionError(t, parseSectionAddressTable, []string{""})
	checkForSectionError(t, parseSectionAddressTable, []string{"10"})
	checkForSectionError(t, parseSectionAddressTable, []string{"10 11", ""})
	checkForSectionError(t, parseSectionAddressTable, []string{"10 11", "MAC1"})
	checkForSectionError(t, parseSectionAddressTable, []string{"10 11", "MAC1 IP1", ""})

	checkForSectionError(t, parseSectionAddressTable, []string{"XX 11", "MAC1 IP1"})
	checkForSectionError(t, parseSectionAddressTable, []string{"10 XX", "MAC1 IP1"})
}

func TestParseSectionDropStatistics_Valid(t *testing.T) {
	lines := []string{
		"10 11",
		"12 13",
	}
	trace := Trace{}
	err := parseSectionDropStatistics(lines, &trace)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedTrace := Trace{
		DroppedPacketsEntry: []*DroppedPacketsEntry{
			&DroppedPacketsEntry{
				Size:  proto.Uint32(10),
				Count: proto.Uint32(11),
			},
			&DroppedPacketsEntry{
				Size:  proto.Uint32(12),
				Count: proto.Uint32(13),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, &trace)
}

func TestParseSectionDropStatistics_Invalid(t *testing.T) {
	checkForSectionError(t, parseSectionDropStatistics, []string{""})
	checkForSectionError(t, parseSectionDropStatistics, []string{"10"})
	checkForSectionError(t, parseSectionDropStatistics, []string{"10 11", ""})
}
