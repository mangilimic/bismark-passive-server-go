package bismarkpassive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

type Section int

const (
	SectionIntro Section = iota
	SectionWhitelist
	SectionAnonymization
	SectionPacketSeries
	SectionFlowTable
	SectionDnsTableA
	SectionDnsTableCname
	SectionAddressTable
	SectionDropStatistics
)

func (section Section) String() string {
	if section == SectionIntro {
		return "intro"
	} else if section == SectionWhitelist {
		return "whitelist"
	} else if section == SectionAnonymization {
		return "anonymization"
	} else if section == SectionPacketSeries {
		return "packet series"
	} else if section == SectionFlowTable {
		return "flow table"
	} else if section == SectionDnsTableA {
		return "DNS A records"
	} else if section == SectionDnsTableCname {
		return "DNS CNAME records"
	} else if section == SectionAddressTable {
		return "MAC addresses"
	} else if section == SectionDropStatistics {
		return "drop statistics"
	}
	return "unknown"
}

type TraceParseError struct {
	Section  Section
	Suberror error
}

func (err *TraceParseError) Error() string {
	return fmt.Sprintf("Section %s %s", err.Section, err.Suberror)
}

func newTraceParseError(section Section, suberror error) error {
	return &TraceParseError{section, suberror}
}

type sectionError struct {
	Message  string
	Suberror error
}

func (err *sectionError) Error() string {
	if err.Suberror == nil {
		return err.Message
	}
	return fmt.Sprintf("%s: %s", err.Message, err.Suberror)
}

func newSectionError(message string, suberror error) error {
	return &sectionError{message, suberror}
}

func linesToSections(lines [][]byte) [][]string {
	sections := [][]string{}
	currentSection := []string{}
	for _, line := range lines {
		if len(line) == 0 {
			sections = append(sections, currentSection)
			currentSection = []string{}
		} else {
			currentSection = append(currentSection, string(line))
		}
	}
	if len(currentSection) > 0 {
		sections = append(sections, currentSection)
	}
	return sections
}

func atoi32(s string) (int32, error) {
	parsed, err := strconv.ParseInt(s, 0, 32)
	if err != nil {
		return 0, err
	}
	return int32(parsed), nil
}

func atou32(str string) (uint32, error) {
	parsed, err := strconv.ParseUint(str, 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

func atoi64(str string) (int64, error) {
	parsed, err := strconv.ParseInt(str, 0, 64)
	if err != nil {
		return 0, err
	}
	return int64(parsed), nil
}

func stringIntToBool(str string) (bool, error) {
	if str == "0" {
		return false, nil
	} else if str == "1" {
		return true, nil
	}
	return false, errors.New("Invalid integer boolean")
}

func words(str string) []string {
	return strings.Split(str, " ")
}

func parseSectionIntro(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", nil)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing file format version", nil)
	}
	if fileFormatVersion, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionError("has invalid file format version", err)
	} else {
		trace.FileFormatVersion = &fileFormatVersion
	}

	if len(sectionLines) < 2 {
		return newSectionError("missing second line", nil)
	}
	secondLineWords := words(sectionLines[1])
	if len(secondLineWords) < 1 {
		return newSectionError("missing build id", nil)
	}
	trace.BuildId = &secondLineWords[0]

	if len(sectionLines) < 3 {
		return newSectionError("missing third line", nil)
	}
	thirdLineWords := words(sectionLines[2])
	if len(thirdLineWords) < 1 {
		return newSectionError("missing node id", nil)
	} else if len(thirdLineWords) < 2 {
		return newSectionError("missing process start time", nil)
	} else if len(thirdLineWords) < 3 {
		return newSectionError("missing sequence number", nil)
	} else if len(thirdLineWords) < 4 {
		return newSectionError("missing trace creation timestamp", nil)
	}
	trace.NodeId = &thirdLineWords[0]
	if processStartTimeMicroseconds, err := atoi64(thirdLineWords[1]); err != nil {
		return newSectionError("has invalid process start time", err)
	} else {
		trace.ProcessStartTimeMicroseconds = &processStartTimeMicroseconds
	}
	if sequenceNumber, err := atoi32(thirdLineWords[2]); err != nil {
		return newSectionError("has invalid sequence number", err)
	} else {
		trace.SequenceNumber = &sequenceNumber
	}
	if traceCreationTimestamp, err := atoi64(thirdLineWords[3]); err != nil {
		return newSectionError("has invalid trace creation timestamp", err)
	} else {
		trace.TraceCreationTimestamp = &traceCreationTimestamp
	}

	if len(sectionLines) < 4 {
		// Missing PCAP statistics is ok, for backwards compatibility.
		return nil
	}
	fourthLineWords := words(sectionLines[3])
	if len(fourthLineWords) < 1 {
		return newSectionError("missing PCAP received", nil)
	} else if len(fourthLineWords) < 2 {
		return newSectionError("missing PCAP dropped", nil)
	} else if len(fourthLineWords) < 3 {
		return newSectionError("missing interface dropped", nil)
	}
	if pcapReceived, err := atou32(fourthLineWords[0]); err != nil {
		return newSectionError("invalid PCAP received", err)
	} else {
		trace.PcapReceived = &pcapReceived
	}
	if pcapDropped, err := atou32(fourthLineWords[1]); err != nil {
		return newSectionError("invalid PCAP dropped", err)
	} else {
		trace.PcapDropped = &pcapDropped
	}
	if interfaceDropped, err := atou32(fourthLineWords[2]); err != nil {
		return newSectionError("invalid interface dropped", err)
	} else {
		trace.InterfaceDropped = &interfaceDropped
	}

	return nil
}

func parseSectionWhitelist(sectionLines []string, trace *Trace) error {
	trace.Whitelist = sectionLines
	return nil
}

func parseSectionAnonymization(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing anonymization signature section", nil)
	}
	if sectionLines[0] != "UNANONYMIZED" {
		trace.AnonymizationSignature = &sectionLines[0]
	}
	return nil
}

func parseSectionPacketSeries(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", nil)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing base timestamp", nil)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing dropped packets count", nil)
	}
	currentTimestampMicroseconds, err := atoi64(firstLineWords[0])
	if err != nil {
		return newSectionError("invalid base timestamp", err)
	}
	if dropped, err := atou32(firstLineWords[1]); err != nil {
		return newSectionError("invalid dropped packet count", err)
	} else {
		trace.PacketSeriesDropped = &dropped
	}

	trace.PacketSeries = make([]*PacketSeriesEntry, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing offset in packet entry", nil)
		} else if len(entryWords) < 2 {
			return newSectionError("missing size in packet entry", nil)
		} else if len(entryWords) < 3 {
			return newSectionError("missing flow id in packet entry", nil)
		}
		if offset, err := atoi32(entryWords[0]); err != nil {
			return newSectionError("invalid offset in packet entry", err)
		} else {
			currentTimestampMicroseconds += int64(offset)
		}
		timestampMicroseconds := currentTimestampMicroseconds
		size, err := atoi32(entryWords[1])
		if err != nil {
			return newSectionError("invalid size in packet entry", err)
		}
		flowId, err := atoi32(entryWords[2])
		if err != nil {
			return newSectionError("invalid flow id in packet entry", err)
		}
		newEntry := PacketSeriesEntry{
			TimestampMicroseconds: &timestampMicroseconds,
			Size:                  &size,
			FlowId:                &flowId,
		}
		trace.PacketSeries[index] = &newEntry
	}
	return nil
}

func parseSectionFlowTable(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", nil)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing base timestamp", nil)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing table size", nil)
	} else if len(firstLineWords) < 3 {
		return newSectionError("missing expiration time", nil)
	} else if len(firstLineWords) < 4 {
		return newSectionError("missing dropped entries count", nil)
	}
	if baseline, err := atoi64(firstLineWords[0]); err != nil {
		return newSectionError("invalid base timestamp", err)
	} else {
		trace.FlowTableBaseline = &baseline
	}
	if tableSize, err := atou32(firstLineWords[1]); err != nil {
		return newSectionError("invalid table size", err)
	} else {
		trace.FlowTableSize = &tableSize
	}
	if expired, err := atoi32(firstLineWords[2]); err != nil {
		return newSectionError("invalid expired count", err)
	} else {
		trace.FlowTableExpired = &expired
	}
	if dropped, err := atoi32(firstLineWords[3]); err != nil {
		return newSectionError("invalid dropped count", err)
	} else {
		trace.FlowTableDropped = &dropped
	}

	trace.FlowTableEntry = make([]*FlowTableEntry, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing flow id from flow table entry", nil)
		} else if len(entryWords) < 2 {
			return newSectionError("missing source IP anonymized from flow table entry", nil)
		} else if len(entryWords) < 3 {
			return newSectionError("missing source IP from flow table entry", nil)
		} else if len(entryWords) < 4 {
			return newSectionError("missing destination IP anonymized from flow table entry", nil)
		} else if len(entryWords) < 5 {
			return newSectionError("missing destination IP from flow table entry", nil)
		} else if len(entryWords) < 6 {
			return newSectionError("missing transport protocol from flow table entry", nil)
		} else if len(entryWords) < 7 {
			return newSectionError("missing source port from flow table entry", nil)
		} else if len(entryWords) < 8 {
			return newSectionError("missing destination port from flow table entry", nil)
		}
		newEntry := FlowTableEntry{}
		if flowId, err := atoi32(entryWords[0]); err != nil {
			return newSectionError("invalid flow id in flow table entry", err)
		} else {
			newEntry.FlowId = &flowId
		}
		if sourceIpAnonymized, err := stringIntToBool(entryWords[1]); err != nil {
			return newSectionError("invalid source IP anonymized", err)
		} else {
			newEntry.SourceIpAnonymized = &sourceIpAnonymized
		}
		newEntry.SourceIp = &entryWords[2]
		if destinationIpAnonymized, err := stringIntToBool(entryWords[3]); err != nil {
			return newSectionError("invalid destination IP anonymized", err)
		} else {
			newEntry.DestinationIpAnonymized = &destinationIpAnonymized
		}
		newEntry.DestinationIp = &entryWords[4]
		if transportProtocol, err := atoi32(entryWords[5]); err != nil {
			return newSectionError("invalid transport protocol", err)
		} else {
			newEntry.TransportProtocol = &transportProtocol
		}
		if sourcePort, err := atoi32(entryWords[6]); err != nil {
			return newSectionError("invalid source port", err)
		} else {
			newEntry.SourcePort = &sourcePort
		}
		if destinationPort, err := atoi32(entryWords[7]); err != nil {
			return newSectionError("invalid destination port", err)
		} else {
			newEntry.DestinationPort = &destinationPort
		}
		trace.FlowTableEntry[index] = &newEntry
	}
	return nil
}

func parseSectionDnsTableA(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", nil)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing dropped DNS A records", nil)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing dropped DNS CNAME records", nil)
	}
	if droppedA, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionError("invalid dropped A records count", err)
	} else {
		trace.ARecordsDropped = &droppedA
	}
	if droppedCname, err := atoi32(firstLineWords[1]); err != nil {
		return newSectionError("invalid dropped CNAME records count", err)
	} else {
		trace.CnameRecordsDropped = &droppedCname
	}

	trace.ARecord = make([]*DnsARecord, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing packet id in record", nil)
		} else if len(entryWords) < 2 {
			return newSectionError("missing address id in record", nil)
		} else if len(entryWords) < 3 {
			return newSectionError("missing anonymized in record", nil)
		} else if len(entryWords) < 4 {
			return newSectionError("missing domain in record", nil)
		} else if len(entryWords) < 5 {
			return newSectionError("missing IP address in record", nil)
		} else if len(entryWords) < 6 {
			return newSectionError("missing TTL id in record", nil)
		}
		newEntry := DnsARecord{}
		if packetId, err := atoi32(entryWords[0]); err != nil {
			return newSectionError("invalid packet id in record", err)
		} else {
			newEntry.PacketId = &packetId
		}
		if addressId, err := atoi32(entryWords[1]); err != nil {
			return newSectionError("invalid address id in record", err)
		} else {
			newEntry.AddressId = &addressId
		}
		if anonymized, err := stringIntToBool(entryWords[2]); err != nil {
			return newSectionError("invalid anonymized in record", err)
		} else {
			newEntry.Anonymized = &anonymized
		}
		newEntry.Domain = &entryWords[3]
		newEntry.IpAddress = &entryWords[4]
		if ttl, err := atoi32(entryWords[5]); err != nil {
			return newSectionError("invalid TTL in record", err)
		} else {
			newEntry.Ttl = &ttl
		}
		trace.ARecord[index] = &newEntry
	}
	return nil
}

func parseSectionDnsTableCname(sectionLines []string, trace *Trace) error {
	trace.CnameRecord = make([]*DnsCnameRecord, len(sectionLines))
	for index, line := range sectionLines {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing packet id in record", nil)
		} else if len(entryWords) < 2 {
			return newSectionError("missing address id in record", nil)
		} else if len(entryWords) < 3 {
			return newSectionError("missing domain anonymized in record", nil)
		} else if len(entryWords) < 4 {
			return newSectionError("missing domain in record", nil)
		} else if len(entryWords) < 5 {
			return newSectionError("missing CNAME anonymized in record", nil)
		} else if len(entryWords) < 6 {
			return newSectionError("missing CNAME in record", nil)
		} else if len(entryWords) < 7 {
			return newSectionError("missing TTL id in record", nil)
		}
		newEntry := DnsCnameRecord{}
		if packetId, err := atoi32(entryWords[0]); err != nil {
			return newSectionError("invalid packet id in record", err)
		} else {
			newEntry.PacketId = &packetId
		}
		if addressId, err := atoi32(entryWords[1]); err != nil {
			return newSectionError("invalid address id in record", err)
		} else {
			newEntry.AddressId = &addressId
		}
		if domainAnonymized, err := stringIntToBool(entryWords[2]); err != nil {
			return newSectionError("invalid domain anonymized in record", err)
		} else {
			newEntry.DomainAnonymized = &domainAnonymized
		}
		newEntry.Domain = &entryWords[3]
		if cnameAnonymized, err := stringIntToBool(entryWords[4]); err != nil {
			return newSectionError("invalid CNAME anonymized in record", err)
		} else {
			newEntry.CnameAnonymized = &cnameAnonymized
		}
		newEntry.Cname = &entryWords[5]
		if ttl, err := atoi32(entryWords[6]); err != nil {
			return newSectionError("invalid TTL in record", err)
		} else {
			newEntry.Ttl = &ttl
		}
		trace.CnameRecord[index] = &newEntry
	}
	return nil
}

func parseSectionAddressTable(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", nil)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing first id", nil)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing table size", nil)
	}
	if firstId, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionError("invalid first id", err)
	} else {
		trace.AddressTableFirstId = &firstId
	}
	if size, err := atoi32(firstLineWords[1]); err != nil {
		return newSectionError("invalid table size", err)
	} else {
		trace.AddressTableSize = &size
	}

	trace.AddressTableEntry = make([]*AddressTableEntry, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing MAC address in entry", nil)
		} else if len(entryWords) < 2 {
			return newSectionError("missing IP address in entry", nil)
		}
		trace.AddressTableEntry[index] = &AddressTableEntry{
			MacAddress: &entryWords[0],
			IpAddress:  &entryWords[1],
		}
	}
	return nil
}

func parseSectionDropStatistics(sectionLines []string, trace *Trace) error {
	trace.DroppedPacketsEntry = make([]*DroppedPacketsEntry, len(sectionLines))
	for index, line := range sectionLines {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing size in entry", nil)
		} else if len(entryWords) < 2 {
			return newSectionError("missing drop count in entry", nil)
		}
		newEntry := DroppedPacketsEntry{}
		if size, err := atou32(entryWords[0]); err != nil {
			return newSectionError("invalid size in entry", err)
		} else {
			newEntry.Size = &size
		}
		if count, err := atou32(entryWords[1]); err != nil {
			return newSectionError("invalid count in entry", err)
		} else {
			newEntry.Count = &count
		}
		trace.DroppedPacketsEntry[index] = &newEntry
	}
	return nil
}

func makeTraceFromSections(sections [][]string) (*Trace, error) {
	type sectionParser func([]string, *Trace) error

	trace := new(Trace)

	mandatorySectionsMap := map[Section]sectionParser{
		SectionIntro:         parseSectionIntro,
		SectionWhitelist:     parseSectionWhitelist,
		SectionAnonymization: parseSectionAnonymization,
		SectionPacketSeries:  parseSectionPacketSeries,
		SectionFlowTable:     parseSectionFlowTable,
		SectionDnsTableA:     parseSectionDnsTableA,
		SectionDnsTableCname: parseSectionDnsTableCname,
		SectionAddressTable:  parseSectionAddressTable,
	}
	for section, parse := range mandatorySectionsMap {
		if len(sections) <= int(section) {
			return nil, newTraceParseError(section, newSectionError("missing", nil))
		}
		if err := parse(sections[int(section)], trace); err != nil {
			return nil, newTraceParseError(section, err)
		}
	}

	optionalSectionsMap := map[Section]sectionParser{
		SectionDropStatistics: parseSectionDropStatistics,
	}
	for section, parse := range optionalSectionsMap {
		if len(sections) <= int(section) {
			continue
		}
		if err := parse(sections[int(section)], trace); err != nil {
			return nil, newTraceParseError(section, err)
		}
	}

	return trace, nil
}

func ParseTrace(source io.Reader) (*Trace, error) {
	contents, err := ioutil.ReadAll(source)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(contents, []byte{'\n'})
	sections := linesToSections(lines)
	trace, err := makeTraceFromSections(sections)
	if err != nil {
		return nil, err
	}
	return trace, nil
}
