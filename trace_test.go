package bismarkpassive

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"strings"
	"testing"
)

func printSections(sections [][]string, lineNumbers []int) {
	fmt.Println("{")
	for i, section := range sections {
		fmt.Println(" {")
		for j, line := range section {
			fmt.Printf("  %d: %s\n", lineNumbers[i]+j+1, line)
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
	//   1: hello
	//   2: world
	//  }
	//  {
	//   4: test
	//  }
	// }
}

func ExampleLinesToSections_empty() {
	lines := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte(""),
		[]byte(""),
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
		[]byte("test4"),
		[]byte(""),
		[]byte("again1"),
		[]byte("again2"),
	}
	printSections(linesToSections(lines))

	// Output:
	// {
	//  {
	//   1: hello
	//   2: world
	//  }
	//  {
	//  }
	//  {
	//   5: test1
	//   6: test2
	//   7: test3
	//   8: test4
	//  }
	//  {
	//   10: again1
	//   11: again2
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
	//   1: hello
	//   2: world
	//  }
	// }
}

func checkForSectionError(t *testing.T, parseSection func([]string, *Trace) error, lines []string, lineNumber int) {
	trace := Trace{}
	err := parseSection(lines, &trace)
	if err == nil {
		t.Fatalf("Trace should be invalid. Instead got trace: %s", trace)
	}
	e, ok := err.(*sectionError)
	if !ok {
		t.Fatalf("Should return sectionError. Instead got error: %s", e)
	}
	if e.LineNumber+1 != lineNumber {
		t.Fatalf("Expected error on line number %d. Got line %d instead", lineNumber, e.LineNumber+1)
	}
}

func checkProtosEqual(t *testing.T, expectedTrace *Trace, trace *Trace) {
	if !proto.Equal(expectedTrace, trace) {
		t.Fatalf("Protocol buffers not equal:\nExpected: %s\nActual:   %s", expectedTrace, trace)
	}
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

func TestParseSectionIntro_Invalid(t *testing.T) {
	// Incompleteness
	checkForSectionError(t, parseSectionIntro, []string{}, 1)
	checkForSectionError(t, parseSectionIntro, []string{""}, 1)
	checkForSectionError(t, parseSectionIntro, []string{"10"}, 2)
	checkForSectionError(t, parseSectionIntro, []string{"10", ""}, 2)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", ""}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", ""}, 4)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76"}, 4)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76 85"}, 4)

	// Integer conversion
	checkForSectionError(t, parseSectionIntro, []string{"STR", "BUILDID", "NODEID 123 321 789", "76 85 99"}, 1)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID STR 321 789", "76 85 99"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 STR 789", "76 85 99"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 STR", "76 85 99"}, 3)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "STR 85 99"}, 4)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76 STR 99"}, 4)
	checkForSectionError(t, parseSectionIntro, []string{"10", "BUILDID", "NODEID 123 321 789", "76 85 STR"}, 4)
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
	checkForSectionError(t, parseSectionPacketSeries, []string{}, 1)
	checkForSectionError(t, parseSectionPacketSeries, []string{""}, 1)
	checkForSectionError(t, parseSectionPacketSeries, []string{"10"}, 1)
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0"}, 2)
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10"}, 2)
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10 23", ""}, 3)
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10 23", "20"}, 3)
	checkForSectionError(t, parseSectionPacketSeries, []string{"10 11", "0 10 23", "20 12"}, 3)
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
	checkForSectionError(t, parseSectionFlowTable, []string{}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{""}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", ""}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 16"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 16 17", ""}, 3)

	checkForSectionError(t, parseSectionFlowTable, []string{"XX 11 12 13", "14 0 IP1 0 IP2 15 16 17"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 XX 12 13", "14 0 IP1 0 IP2 15 16 17"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 XX 13", "14 0 IP1 0 IP2 15 16 17"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 XX", "14 0 IP1 0 IP2 15 16 17"}, 1)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "XX 0 IP1 0 IP2 15 16 17"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 X IP1 0 IP2 15 16 17"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 X IP2 15 16 17"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 XX 16 17"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 XX 17"}, 2)
	checkForSectionError(t, parseSectionFlowTable, []string{"10 11 12 13", "14 0 IP1 0 IP2 15 16 XX"}, 2)
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
	checkForSectionError(t, parseSectionDnsTableA, []string{}, 1)
	checkForSectionError(t, parseSectionDnsTableA, []string{""}, 1)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10"}, 1)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", ""}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1 IP1"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1 IP1 14", ""}, 3)

	checkForSectionError(t, parseSectionDnsTableA, []string{"XX 11", "12 13 0 DOM1 IP1 14"}, 1)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 XX", "12 13 0 DOM1 IP1 14"}, 1)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "XX 13 0 DOM1 IP1 14"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 XX 0 DOM1 IP1 14"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 X DOM1 IP1 14"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 2 DOM1 IP1 14"}, 2)
	checkForSectionError(t, parseSectionDnsTableA, []string{"10 11", "12 13 0 DOM1 IP1 XX"}, 2)
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

func TestParseSectionDnsTableCname_ValidVersion2(t *testing.T) {
	lines := []string{
		"12 13 0 DOM1 CN1 14",
		"15 16 0 DOM2 CN2 17",
		"18 19 1 DOM3 CN3 20",
		"21 22 1 DOM4 CN4 23",
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
				CnameAnonymized:  proto.Bool(false),
				Cname:            proto.String("CN2"),
				Ttl:              proto.Int32(17),
			},
			&DnsCnameRecord{
				PacketId:         proto.Int32(18),
				AddressId:        proto.Int32(19),
				DomainAnonymized: proto.Bool(true),
				Domain:           proto.String("DOM3"),
				CnameAnonymized:  proto.Bool(true),
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
	checkForSectionError(t, parseSectionDnsTableCname, []string{""}, 1)
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12"}, 1)
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13"}, 1)
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0"}, 1)
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1"}, 1)
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1 0 CN1"}, 1)
	checkForSectionError(t, parseSectionDnsTableCname, []string{"12 13 0 DOM1 0 CN1 14", ""}, 2)
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
	checkForSectionError(t, parseSectionAddressTable, []string{}, 1)
	checkForSectionError(t, parseSectionAddressTable, []string{""}, 1)
	checkForSectionError(t, parseSectionAddressTable, []string{"10"}, 1)
	checkForSectionError(t, parseSectionAddressTable, []string{"10 11", ""}, 2)
	checkForSectionError(t, parseSectionAddressTable, []string{"10 11", "MAC1"}, 2)
	checkForSectionError(t, parseSectionAddressTable, []string{"10 11", "MAC1 IP1", ""}, 3)

	checkForSectionError(t, parseSectionAddressTable, []string{"XX 11", "MAC1 IP1"}, 1)
	checkForSectionError(t, parseSectionAddressTable, []string{"10 XX", "MAC1 IP1"}, 1)
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

func TestParseSectionDropStatistics_SpecialCase(t *testing.T) {
	lines := []string{
		"10 11",
		"12 13",
		"0 ",
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
	checkForSectionError(t, parseSectionDropStatistics, []string{""}, 1)
	checkForSectionError(t, parseSectionDropStatistics, []string{"10"}, 1)
	checkForSectionError(t, parseSectionDropStatistics, []string{"10 11", ""}, 2)
}

func TestParseTrace_Valid(t *testing.T) {
	fileContents :=
		`5
UNKNOWN
OWC43DC78EE081 1346479358582428 0 1346479388
138 0 0

google.com
facebook.com
youtube.com
yahoo.com
amazon.com
wikipedia.org
ebay.com
twitter.com

88698fe15783cd75107714ef91761673c41a7d1f

1346479359146721 0
0 74 31646
498 74 25349
50825 174 34629
12323 174 36522

1346479359 34 0 0
829 1 9ccddd49ad2336c5 1 ebd6aae385287e9f 6 5228 46716
9382 1 8f1b0f1764a0dd3e 1 ebd6aae385287e9f 6 993 52519

0 0
2 0 0 www.l.google.com 950a8fca863ac696 298

2 0 0 www.google.com 0 www.l.google.com 43198

0 256
64a769ccb29d ebd6aae385287e9f
c43dc79106a8 1336ec0318683863


`
	reader := strings.NewReader(fileContents)
	if reader == nil {
		t.Fatal("Could not create reader")
	}
	trace, err := ParseTrace(reader)
	if err != nil {
		t.Fatalf("Failed to parse trace: %s", err)
	}
	expectedTrace := Trace{
		FileFormatVersion: proto.Int32(5),
		BuildId:           proto.String("UNKNOWN"),
		NodeId:            proto.String("OWC43DC78EE081"),
		ProcessStartTimeMicroseconds: proto.Int64(1346479358582428),
		SequenceNumber:               proto.Int32(0),
		TraceCreationTimestamp:       proto.Int64(1346479388),
		PcapReceived:                 proto.Uint32(138),
		PcapDropped:                  proto.Uint32(0),
		InterfaceDropped:             proto.Uint32(0),
		Whitelist: []string{
			"google.com",
			"facebook.com",
			"youtube.com",
			"yahoo.com",
			"amazon.com",
			"wikipedia.org",
			"ebay.com",
			"twitter.com",
		},
		AnonymizationSignature: proto.String("88698fe15783cd75107714ef91761673c41a7d1f"),
		PacketSeriesDropped:    proto.Uint32(0),
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1346479359146721),
				Size:                  proto.Int32(74),
				FlowId:                proto.Int32(31646),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1346479359147219),
				Size:                  proto.Int32(74),
				FlowId:                proto.Int32(25349),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1346479359198044),
				Size:                  proto.Int32(174),
				FlowId:                proto.Int32(34629),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1346479359210367),
				Size:                  proto.Int32(174),
				FlowId:                proto.Int32(36522),
			},
		},
		FlowTableBaseline: proto.Int64(1346479359),
		FlowTableSize:     proto.Uint32(34),
		FlowTableExpired:  proto.Int32(0),
		FlowTableDropped:  proto.Int32(0),
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:                  proto.Int32(829),
				SourceIpAnonymized:      proto.Bool(true),
				SourceIp:                proto.String("9ccddd49ad2336c5"),
				DestinationIpAnonymized: proto.Bool(true),
				DestinationIp:           proto.String("ebd6aae385287e9f"),
				TransportProtocol:       proto.Int32(6),
				SourcePort:              proto.Int32(5228),
				DestinationPort:         proto.Int32(46716),
			},
			&FlowTableEntry{
				FlowId:                  proto.Int32(9382),
				SourceIpAnonymized:      proto.Bool(true),
				SourceIp:                proto.String("8f1b0f1764a0dd3e"),
				DestinationIpAnonymized: proto.Bool(true),
				DestinationIp:           proto.String("ebd6aae385287e9f"),
				TransportProtocol:       proto.Int32(6),
				SourcePort:              proto.Int32(993),
				DestinationPort:         proto.Int32(52519),
			},
		},
		ARecordsDropped:     proto.Int32(0),
		CnameRecordsDropped: proto.Int32(0),
		ARecord: []*DnsARecord{
			&DnsARecord{
				PacketId:   proto.Int32(2),
				AddressId:  proto.Int32(0),
				Anonymized: proto.Bool(false),
				Domain:     proto.String("www.l.google.com"),
				IpAddress:  proto.String("950a8fca863ac696"),
				Ttl:        proto.Int32(298),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				PacketId:         proto.Int32(2),
				AddressId:        proto.Int32(0),
				DomainAnonymized: proto.Bool(false),
				Domain:           proto.String("www.google.com"),
				CnameAnonymized:  proto.Bool(false),
				Cname:            proto.String("www.l.google.com"),
				Ttl:              proto.Int32(43198),
			},
		},
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(256),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("64a769ccb29d"),
				IpAddress:  proto.String("ebd6aae385287e9f"),
			},
			&AddressTableEntry{
				MacAddress: proto.String("c43dc79106a8"),
				IpAddress:  proto.String("1336ec0318683863"),
			},
		},
	}
	checkProtosEqual(t, &expectedTrace, trace)
}

func checkForParseError(t *testing.T, fileContents string, expectedLineNumber int) {
	reader := strings.NewReader(fileContents)
	if reader == nil {
		t.Fatal("Could not create reader")
	}
	_, err := ParseTrace(reader)
	if err == nil {
		t.Fatalf("Trace should have failed to parse")
	}
	e, ok := err.(*TraceParseError)
	if !ok {
		t.Fatalf("ParseTrace should have failed with a TraceParseError. Instad failed with: %s", e)
	}
	if expectedLineNumber >= 0 && e.LineNumber != expectedLineNumber {
		t.Fatalf("Expected TraceParseError on line %d. Got line %d instead", expectedLineNumber, e.LineNumber)
	}
}

func TestParseTrace_Invalid(t *testing.T) {
	invalidVersionContents :=
		`InvalidVersion
UNKNOWN
OWC43DC78EE081 1346479358582428 0 1346479388
138 0 0

google.com
facebook.com
youtube.com
yahoo.com
amazon.com
wikipedia.org
ebay.com
twitter.com

88698fe15783cd75107714ef91761673c41a7d1f

1346479359146721 0
0 74 31646
498 74 25349
50825 174 34629
12323 174 36522

1346479359 34 0 0
829 1 9ccddd49ad2336c5 1 ebd6aae385287e9f 6 5228 46716
9382 1 8f1b0f1764a0dd3e 1 ebd6aae385287e9f 6 993 52519

0 0
2 0 0 www.l.google.com 950a8fca863ac696 298

2 0 0 www.google.com 0 www.l.google.com 43198

0 256
64a769ccb29d ebd6aae385287e9f
c43dc79106a8 1336ec0318683863


`
	checkForParseError(t, invalidVersionContents, 0)

	invalidBaseTimestampContents :=
		`5
UNKNOWN
OWC43DC78EE081 1346479358582428 0 1346479388
138 0 0

google.com
facebook.com
youtube.com
yahoo.com
amazon.com
wikipedia.org
ebay.com
twitter.com

88698fe15783cd75107714ef91761673c41a7d1f

InvalidBaseTimestamp 0
0 74 31646
498 74 25349
50825 174 34629
12323 174 36522

1346479359 34 0 0
829 1 9ccddd49ad2336c5 1 ebd6aae385287e9f 6 5228 46716
9382 1 8f1b0f1764a0dd3e 1 ebd6aae385287e9f 6 993 52519

0 0
2 0 0 www.l.google.com 950a8fca863ac696 298

2 0 0 www.google.com 0 www.l.google.com 43198

0 256
64a769ccb29d ebd6aae385287e9f
c43dc79106a8 1336ec0318683863


`
	checkForParseError(t, invalidBaseTimestampContents, 16)

	invalidAddressTableIndex :=
		`5
UNKNOWN
OWC43DC78EE081 1346479358582428 0 1346479388
138 0 0

google.com
facebook.com
youtube.com
yahoo.com
amazon.com
wikipedia.org
ebay.com
twitter.com

88698fe15783cd75107714ef91761673c41a7d1f

1346479359146721 0
0 74 31646
498 74 25349
50825 174 34629
12323 174 36522

1346479359 34 0 0
829 1 9ccddd49ad2336c5 1 ebd6aae385287e9f 6 5228 46716
9382 1 8f1b0f1764a0dd3e 1 ebd6aae385287e9f 6 993 52519

0 0
2 0 0 www.l.google.com 950a8fca863ac696 298

2 0 0 www.google.com 0 www.l.google.com 43198

InvalidIndex 256
64a769ccb29d ebd6aae385287e9f
c43dc79106a8 1336ec0318683863


`
	checkForParseError(t, invalidAddressTableIndex, 31)
}
