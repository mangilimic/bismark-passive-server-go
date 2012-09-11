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
	Section    Section
	LineNumber int
	Suberror   error
}

func (err *TraceParseError) Error() string {
	if err.LineNumber >= 0 {
		return fmt.Sprintf("Section %s (line %d) %s", err.Section, err.LineNumber+1, err.Suberror)
	}
	return fmt.Sprintf("Section %s %s", err.Section, err.Suberror)
}

func newTraceParseError(section Section, lineNumber int, suberror error) error {
	return &TraceParseError{
		Section:    section,
		LineNumber: lineNumber,
		Suberror:   suberror,
	}
}

type sectionError struct {
	Message    string
	LineNumber int
	Suberror   error
	Example    *string
}

func (err *sectionError) Error() string {
	if err.Suberror == nil {
		return err.Message
	}
	if err.Example == nil {
		return fmt.Sprintf("%s: %s", err.Message, err.Suberror)
	}
	return fmt.Sprintf("%s (\"%s\"): %s", err.Message, *err.Example, err.Suberror)
}

func newSectionConversionError(message string, lineNumber int, example string, suberror error) error {
	return &sectionError{
		Message:    message,
		LineNumber: lineNumber,
		Suberror:   suberror,
		Example:    &example,
	}
}

func newSectionErrorWithSuberror(message string, lineNumber int, suberror error) error {
	return &sectionError{
		Message:    message,
		LineNumber: lineNumber,
		Suberror:   suberror,
	}
}

func newSectionError(message string, lineNumber int) error {
	return newSectionErrorWithSuberror(message, lineNumber, nil)
}

func linesToSections(lines [][]byte) (sections [][]string, lineNumbers []int) {
	currentSection := []string{}
	lineNumbers = append(lineNumbers, 0)
	for lineNumber, line := range lines {
		if len(line) == 0 {
			sections = append(sections, currentSection)
			lineNumbers = append(lineNumbers, lineNumber+1)
			currentSection = []string{}
		} else {
			currentSection = append(currentSection, string(line))
		}
	}
	if len(currentSection) > 0 {
		sections = append(sections, currentSection)
	}
	return
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
		return newSectionError("missing first line", 0)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing file format version", 0)
	}
	if fileFormatVersion, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionConversionError("has invalid file format version", 0, firstLineWords[0], err)
	} else {
		trace.FileFormatVersion = &fileFormatVersion
	}

	if len(sectionLines) < 2 {
		return newSectionError("missing second line", 1)
	}
	if len(sectionLines[1]) == 0 {
		return newSectionError("missing build id", 1)
	}
	trace.BuildId = &sectionLines[1]

	if len(sectionLines) < 3 {
		return newSectionError("missing third line", 2)
	}
	thirdLineWords := words(sectionLines[2])
	if len(thirdLineWords) < 1 {
		return newSectionError("missing node id", 2)
	} else if len(thirdLineWords) < 2 {
		return newSectionError("missing process start time", 2)
	} else if len(thirdLineWords) < 3 {
		return newSectionError("missing sequence number", 2)
	} else if len(thirdLineWords) < 4 {
		return newSectionError("missing trace creation timestamp", 2)
	}
	trace.NodeId = &thirdLineWords[0]
	if processStartTimeMicroseconds, err := atoi64(thirdLineWords[1]); err != nil {
		return newSectionConversionError("has invalid process start time", 2, thirdLineWords[1], err)
	} else {
		trace.ProcessStartTimeMicroseconds = &processStartTimeMicroseconds
	}
	if sequenceNumber, err := atoi32(thirdLineWords[2]); err != nil {
		return newSectionConversionError("has invalid sequence number", 2, thirdLineWords[2], err)
	} else {
		trace.SequenceNumber = &sequenceNumber
	}
	if traceCreationTimestamp, err := atoi64(thirdLineWords[3]); err != nil {
		return newSectionConversionError("has invalid trace creation timestamp", 2, thirdLineWords[3], err)
	} else {
		trace.TraceCreationTimestamp = &traceCreationTimestamp
	}

	if len(sectionLines) < 4 {
		// Missing PCAP statistics is ok, for backwards compatibility.
		return nil
	}
	fourthLineWords := words(sectionLines[3])
	if len(fourthLineWords) < 1 {
		return newSectionError("missing PCAP received", 3)
	} else if len(fourthLineWords) < 2 {
		return newSectionError("missing PCAP dropped", 3)
	} else if len(fourthLineWords) < 3 {
		return newSectionError("missing interface dropped", 3)
	}
	if pcapReceived, err := atou32(fourthLineWords[0]); err != nil {
		return newSectionConversionError("invalid PCAP received", 3, fourthLineWords[0], err)
	} else {
		trace.PcapReceived = &pcapReceived
	}
	if pcapDropped, err := atou32(fourthLineWords[1]); err != nil {
		return newSectionConversionError("invalid PCAP dropped", 3, fourthLineWords[1], err)
	} else {
		trace.PcapDropped = &pcapDropped
	}
	if interfaceDropped, err := atou32(fourthLineWords[2]); err != nil {
		return newSectionConversionError("invalid interface dropped", 3, fourthLineWords[2], err)
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
		return newSectionError("missing anonymization signature section", 0)
	}
	if sectionLines[0] != "UNANONYMIZED" {
		trace.AnonymizationSignature = &sectionLines[0]
	}
	return nil
}

func parseSectionPacketSeries(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", 0)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing base timestamp", 0)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing dropped packets count", 0)
	}
	currentTimestampMicroseconds, err := atoi64(firstLineWords[0])
	if err != nil {
		return newSectionConversionError("invalid base timestamp", 0, firstLineWords[0], err)
	}
	if dropped, err := atou32(firstLineWords[1]); err != nil {
		return newSectionConversionError("invalid dropped packet count", 0, firstLineWords[1], err)
	} else {
		trace.PacketSeriesDropped = &dropped
	}

	trace.PacketSeries = make([]*PacketSeriesEntry, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing offset in packet entry", 1+index)
		} else if len(entryWords) < 2 {
			return newSectionError("missing size in packet entry", 1+index)
		} else if len(entryWords) < 3 {
			return newSectionError("missing flow id in packet entry", 1+index)
		}
		if offset, err := atoi32(entryWords[0]); err != nil {
			return newSectionConversionError("invalid offset in packet entry", 1+index, entryWords[0], err)
		} else {
			currentTimestampMicroseconds += int64(offset)
		}
		timestampMicroseconds := currentTimestampMicroseconds
		size, err := atoi32(entryWords[1])
		if err != nil {
			return newSectionConversionError("invalid size in packet entry", 1+index, entryWords[1], err)
		}
		flowId, err := atoi32(entryWords[2])
		if err != nil {
			return newSectionConversionError("invalid flow id in packet entry", 1+index, entryWords[2], err)
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
		return newSectionError("missing first line", 0)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing base timestamp", 0)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing table size", 0)
	} else if len(firstLineWords) < 3 {
		return newSectionError("missing expiration time", 0)
	} else if len(firstLineWords) < 4 {
		return newSectionError("missing dropped entries count", 0)
	}
	if baseline, err := atoi64(firstLineWords[0]); err != nil {
		return newSectionConversionError("invalid base timestamp", 0, firstLineWords[0], err)
	} else {
		trace.FlowTableBaseline = &baseline
	}
	if tableSize, err := atou32(firstLineWords[1]); err != nil {
		return newSectionConversionError("invalid table size", 0, firstLineWords[1], err)
	} else {
		trace.FlowTableSize = &tableSize
	}
	if expired, err := atoi32(firstLineWords[2]); err != nil {
		return newSectionConversionError("invalid expired count", 0, firstLineWords[2], err)
	} else {
		trace.FlowTableExpired = &expired
	}
	if dropped, err := atoi32(firstLineWords[3]); err != nil {
		return newSectionConversionError("invalid dropped count", 0, firstLineWords[3], err)
	} else {
		trace.FlowTableDropped = &dropped
	}

	trace.FlowTableEntry = make([]*FlowTableEntry, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing flow id from flow table entry", 1+index)
		} else if len(entryWords) < 2 {
			return newSectionError("missing source IP anonymized from flow table entry", 1+index)
		} else if len(entryWords) < 3 {
			return newSectionError("missing source IP from flow table entry", 1+index)
		} else if len(entryWords) < 4 {
			return newSectionError("missing destination IP anonymized from flow table entry", 1+index)
		} else if len(entryWords) < 5 {
			return newSectionError("missing destination IP from flow table entry", 1+index)
		} else if len(entryWords) < 6 {
			return newSectionError("missing transport protocol from flow table entry", 1+index)
		} else if len(entryWords) < 7 {
			return newSectionError("missing source port from flow table entry", 1+index)
		} else if len(entryWords) < 8 {
			return newSectionError("missing destination port from flow table entry", 1+index)
		}
		newEntry := FlowTableEntry{}
		if flowId, err := atoi32(entryWords[0]); err != nil {
			return newSectionConversionError("invalid flow id in flow table entry", 1+index, entryWords[0], err)
		} else {
			newEntry.FlowId = &flowId
		}
		if sourceIpAnonymized, err := stringIntToBool(entryWords[1]); err != nil {
			return newSectionConversionError("invalid source IP anonymized", 1+index, entryWords[1], err)
		} else {
			newEntry.SourceIpAnonymized = &sourceIpAnonymized
		}
		newEntry.SourceIp = &entryWords[2]
		if destinationIpAnonymized, err := stringIntToBool(entryWords[3]); err != nil {
			return newSectionConversionError("invalid destination IP anonymized", 1+index, entryWords[3], err)
		} else {
			newEntry.DestinationIpAnonymized = &destinationIpAnonymized
		}
		newEntry.DestinationIp = &entryWords[4]
		if transportProtocol, err := atoi32(entryWords[5]); err != nil {
			return newSectionConversionError("invalid transport protocol", 1+index, entryWords[5], err)
		} else {
			newEntry.TransportProtocol = &transportProtocol
		}
		if sourcePort, err := atoi32(entryWords[6]); err != nil {
			return newSectionConversionError("invalid source port", 1+index, entryWords[6], err)
		} else {
			newEntry.SourcePort = &sourcePort
		}
		if destinationPort, err := atoi32(entryWords[7]); err != nil {
			return newSectionConversionError("invalid destination port", 1+index, entryWords[7], err)
		} else {
			newEntry.DestinationPort = &destinationPort
		}
		trace.FlowTableEntry[index] = &newEntry
	}
	return nil
}

func parseSectionDnsTableA(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", 0)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing dropped DNS A records", 0)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing dropped DNS CNAME records", 0)
	}
	if droppedA, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionConversionError("invalid dropped A records count", 0, firstLineWords[0], err)
	} else {
		trace.ARecordsDropped = &droppedA
	}
	if droppedCname, err := atoi32(firstLineWords[1]); err != nil {
		return newSectionConversionError("invalid dropped CNAME records count", 0, firstLineWords[1], err)
	} else {
		trace.CnameRecordsDropped = &droppedCname
	}

	trace.ARecord = make([]*DnsARecord, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing packet id in record", 1+index)
		} else if len(entryWords) < 2 {
			return newSectionError("missing address id in record", 1+index)
		} else if len(entryWords) < 3 {
			return newSectionError("missing anonymized in record", 1+index)
		} else if len(entryWords) < 4 {
			return newSectionError("missing domain in record", 1+index)
		} else if len(entryWords) < 5 {
			return newSectionError("missing IP address in record", 1+index)
		} else if len(entryWords) < 6 {
			return newSectionError("missing TTL id in record", 1+index)
		}
		newEntry := DnsARecord{}
		if packetId, err := atoi32(entryWords[0]); err != nil {
			return newSectionConversionError("invalid packet id in record", 1+index, entryWords[0], err)
		} else {
			newEntry.PacketId = &packetId
		}
		if addressId, err := atoi32(entryWords[1]); err != nil {
			return newSectionConversionError("invalid address id in record", 1+index, entryWords[1], err)
		} else {
			newEntry.AddressId = &addressId
		}
		if anonymized, err := stringIntToBool(entryWords[2]); err != nil {
			return newSectionConversionError("invalid anonymized in record", 1+index, entryWords[2], err)
		} else {
			newEntry.Anonymized = &anonymized
		}
		newEntry.Domain = &entryWords[3]
		newEntry.IpAddress = &entryWords[4]
		if ttl, err := atoi32(entryWords[5]); err != nil {
			return newSectionConversionError("invalid TTL in record", 1+index, entryWords[5], err)
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
			return newSectionError("missing packet id in record", index)
		} else if len(entryWords) < 2 {
			return newSectionError("missing address id in record", index)
		} else if len(entryWords) < 3 {
			return newSectionError("missing domain anonymized in record", index)
		} else if len(entryWords) < 4 {
			return newSectionError("missing domain in record", index)
		} else if len(entryWords) < 5 {
			return newSectionError("missing CNAME anonymized (or CNAME) in record", index)
		} else if len(entryWords) < 6 {
			return newSectionError("missing CNAME (or TTL id) in record", index)
		}
		newEntry := DnsCnameRecord{}
		if packetId, err := atoi32(entryWords[0]); err != nil {
			return newSectionConversionError("invalid packet id in record", index, entryWords[0], err)
		} else {
			newEntry.PacketId = &packetId
		}
		if addressId, err := atoi32(entryWords[1]); err != nil {
			return newSectionConversionError("invalid address id in record", index, entryWords[1], err)
		} else {
			newEntry.AddressId = &addressId
		}
		if domainAnonymized, err := stringIntToBool(entryWords[2]); err != nil {
			return newSectionConversionError("invalid domain anonymized in record", index, entryWords[2], err)
		} else {
			newEntry.DomainAnonymized = &domainAnonymized
		}
		newEntry.Domain = &entryWords[3]
		if len(entryWords) == 6 {
			newEntry.CnameAnonymized = newEntry.DomainAnonymized
		} else if len(entryWords) >= 7 {
			if cnameAnonymized, err := stringIntToBool(entryWords[4]); err != nil {
				return newSectionConversionError("invalid CNAME anonymized in record", index, entryWords[4], err)
			} else {
				newEntry.CnameAnonymized = &cnameAnonymized
			}
		} else {
			panic("Trace parser error in CNAME section.")
		}
		newEntry.Cname = &entryWords[len(entryWords) - 2]
		if ttl, err := atoi32(entryWords[len(entryWords) - 1]); err != nil {
			return newSectionConversionError("invalid TTL in record", index, entryWords[len(entryWords) - 1], err)
		} else {
			newEntry.Ttl = &ttl
		}
		trace.CnameRecord[index] = &newEntry
	}
	return nil
}

func parseSectionAddressTable(sectionLines []string, trace *Trace) error {
	if len(sectionLines) < 1 {
		return newSectionError("missing first line", 0)
	}
	firstLineWords := words(sectionLines[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing first id", 0)
	} else if len(firstLineWords) < 2 {
		return newSectionError("missing table size", 0)
	}
	if firstId, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionConversionError("invalid first id", 0, firstLineWords[0], err)
	} else {
		trace.AddressTableFirstId = &firstId
	}
	if size, err := atoi32(firstLineWords[1]); err != nil {
		return newSectionConversionError("invalid table size", 0, firstLineWords[1], err)
	} else {
		trace.AddressTableSize = &size
	}

	trace.AddressTableEntry = make([]*AddressTableEntry, len(sectionLines[1:]))
	for index, line := range sectionLines[1:] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing MAC address in entry", 1+index)
		} else if len(entryWords) < 2 {
			return newSectionError("missing IP address in entry", 1+index)
		}
		trace.AddressTableEntry[index] = &AddressTableEntry{
			MacAddress: &entryWords[0],
			IpAddress:  &entryWords[1],
		}
	}
	return nil
}

func parseSectionDropStatistics(sectionLines []string, trace *Trace) error {
	numLines := len(sectionLines)
	// Compensate for a bug where some traces don't leave space for
	// a dropped packets section and skip to an HTTP URLs section.
	for index, line := range sectionLines {
		if line != "" && line[len(line) - 1] == ' ' {
			numLines = index
		}
	}
	trace.DroppedPacketsEntry = make([]*DroppedPacketsEntry, numLines)
	for index, line := range sectionLines[:numLines] {
		entryWords := words(line)
		if len(entryWords) < 1 {
			return newSectionError("missing size in entry", index)
		} else if len(entryWords) < 2 {
			return newSectionError("missing drop count in entry", index)
		}
		newEntry := DroppedPacketsEntry{}
		if size, err := atou32(entryWords[0]); err != nil {
			return newSectionConversionError("invalid size in entry", index, entryWords[0], err)
		} else {
			newEntry.Size = &size
		}
		if count, err := atou32(entryWords[1]); err != nil {
			return newSectionConversionError("invalid count in entry", index, entryWords[1], err)
		} else {
			newEntry.Count = &count
		}
		trace.DroppedPacketsEntry[index] = &newEntry
	}
	return nil
}

func makeTraceFromSections(sections [][]string, lineNumbers []int) (*Trace, error) {
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
			return nil, newTraceParseError(section, 0, newSectionError("missing", 0))
		}
		if err := parse(sections[int(section)], trace); err != nil {
			if e, ok := err.(*sectionError); ok {
				return nil, newTraceParseError(section, lineNumbers[int(section)]+e.LineNumber, err)
			} else {
				return nil, newTraceParseError(section, -1, err)
			}
		}
	}

	optionalSectionsMap := map[Section]sectionParser{
		SectionDropStatistics: parseSectionDropStatistics,
	}
	for section, parse := range optionalSectionsMap {
		if int(section) >= len(sections) {
			continue
		}
		if err := parse(sections[int(section)], trace); err != nil {
			if e, ok := err.(*sectionError); ok {
				return nil, newTraceParseError(section, lineNumbers[int(section)]+e.LineNumber, err)
			} else {
				return nil, newTraceParseError(section, -1, err)
			}
		}
	}

	// Fill in nil repeated fields, otherise proto serialization fails.
	if trace.PacketSeries == nil {
		trace.PacketSeries = make([]*PacketSeriesEntry, 0)
	}
	if trace.FlowTableEntry == nil {
		trace.FlowTableEntry = make([]*FlowTableEntry, 0)
	}
	if trace.ARecord == nil {
		trace.ARecord = make([]*DnsARecord, 0)
	}
	if trace.CnameRecord == nil {
		trace.CnameRecord = make([]*DnsCnameRecord, 0)
	}
	if trace.AddressTableEntry == nil {
		trace.AddressTableEntry = make([]*AddressTableEntry, 0)
	}
	if trace.DroppedPacketsEntry == nil {
		trace.DroppedPacketsEntry = make([]*DroppedPacketsEntry, 0)
	}
	return trace, nil
}

func ParseTrace(source io.Reader) (*Trace, error) {
	contents, err := ioutil.ReadAll(source)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(contents, []byte{'\n'})
	sections, lineNumbers := linesToSections(lines)
	trace, err := makeTraceFromSections(sections, lineNumbers)
	if err != nil {
		return nil, err
	}
	return trace, nil
}
