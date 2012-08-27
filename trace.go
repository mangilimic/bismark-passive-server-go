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

func (s Section) String() string {
	if s == SectionIntro {
		return "intro"
	} else if s == SectionWhitelist {
		return "whitelist"
	} else if s == SectionAnonymization {
		return "anonymization"
	} else if s == SectionPacketSeries {
		return "packet series"
	} else if s == SectionFlowTable {
		return "flow table"
	} else if s == SectionDnsTableA {
		return "DNS A records"
	} else if s == SectionDnsTableCname {
		return "DNS CNAME records"
	} else if s == SectionAddressTable {
		return "MAC addresses"
	} else if s == SectionDropStatistics {
		return "drop statistics"
	}
	return "unknown"
}

type TraceParseError struct {
	Section  Section
	Suberror error
}

func (e *TraceParseError) Error() string {
	return fmt.Sprintf("Section %s %s", e.Section, e.Suberror)
}

func newTraceParseError(section Section, suberror error) error {
	return &TraceParseError{section, suberror}
}

type sectionError struct {
	Message  string
	Suberror error
}

func (e *sectionError) Error() string {
	if e.Suberror == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Message, e.Suberror)
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

func atou32(s string) (uint32, error) {
	parsed, err := strconv.ParseUint(s, 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

func atoi64(s string) (int64, error) {
	parsed, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return 0, err
	}
	return int64(parsed), nil
}

func words(s string) []string {
	return strings.Split(s, " ")
}

func parseSectionIntro(section []string, trace *Trace) error {
	if len(section) < 1 {
		return newSectionError("missing first line", nil)
	}
	firstLineWords := words(section[0])
	if len(firstLineWords) < 1 {
		return newSectionError("missing file format version", nil)
	}
	if fileFormatVersion, err := atoi32(firstLineWords[0]); err != nil {
		return newSectionError("has invalid file format version", err)
	} else {
		trace.FileFormatVersion = &fileFormatVersion
	}

	if len(section) < 2 {
		return newSectionError("missing second line", nil)
	}
	secondLineWords := words(section[1])
	if len(secondLineWords) < 1 {
		return newSectionError("missing build id", nil)
	}
	trace.BuildId = &secondLineWords[0]

	if len(section) < 3 {
		return newSectionError("missing third line", nil)
	}
	thirdLineWords := words(section[2])
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

	if len(section) < 4 {
		// Missing PCAP statistics is ok, for compatibility.
		return nil
	}
	fourthLineWords := words(section[3])
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
