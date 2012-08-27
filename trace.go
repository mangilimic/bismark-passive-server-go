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

type TraceParseError struct {
    Section Section
    Suberror error
}

func (e *TraceParseError) Error() string {
    return fmt.Sprintf("Section %s %s", e.Section, e.Suberror)
}

func NewTraceParseError(section Section, suberror error) error {
    return &TraceParseError{section, suberror}
}

type sectionError struct {
    Message string
    Suberror error
}

func (e *sectionError) Error() string {
    if (e.Suberror == nil) {
        return e.Message
    }
    return fmt.Sprintf("%s: %s", e.Message, e.Suberror)
}

func newSectionError(message string, suberror error) error {
    return &sectionError{message, suberror};
}

func parseSections(lines [][]byte) ([][]string) {
    sections := [][]string{}
    currentSection := []string{}
    for _, line := range(lines) {
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

func words(s string) []string {
    return strings.Split(s, " ");
}

func parseSectionIntro(section []string, trace *Trace) error {
    if (len(section) < 1) {
        return newSectionError("missing first line", nil);
    }
    introWords := strings.Split(section[0], " ")
    if len(introWords) < 1 {
        return newSectionError("missing file format version", nil);
    }
    fileFormatVersion, err := atoi32(introWords[0])
    if err != nil {
        return newSectionError("has invalid file format version", err);
    }
    trace.FileFormatVersion = &fileFormatVersion
    return nil
}

func makeTraceFromSections(sections [][]string) (*Trace, error) {
    trace := new(Trace)

    type sectionParser func([]string, *Trace) error
    sectionsMap := map[Section] sectionParser {
        SectionIntro: parseSectionIntro,
    }

    for section, parse := range sectionsMap {
        if len(sections) <= int(section) {
            return nil, NewTraceParseError(section, newSectionError("missing", nil))
        }
        sectionErr := parse(sections[int(section)], trace)
        if sectionErr != nil {
            return nil, NewTraceParseError(section, sectionErr)
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
    sections := parseSections(lines)
    trace, err := makeTraceFromSections(sections)
    if err != nil {
        return nil, err
    }
    return trace, nil
}
