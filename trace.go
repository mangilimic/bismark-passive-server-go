package bismarkpassive

import (
	"bytes"
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
		return newSectionError("has invalid trace creation tiemstamp", err)
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

func makeTraceFromSections(sections [][]string) (*Trace, error) {
	trace := new(Trace)

	type sectionParser func([]string, *Trace) error
	sectionsMap := map[Section]sectionParser{
		SectionIntro: parseSectionIntro,
	}

	for section, parse := range sectionsMap {
		if len(sections) <= int(section) {
			return nil, newTraceParseError(section, newSectionError("missing", nil))
		}
		sectionErr := parse(sections[int(section)], trace)
		if sectionErr != nil {
			return nil, newTraceParseError(section, sectionErr)
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
