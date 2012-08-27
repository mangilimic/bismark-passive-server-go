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

func checkForSectionError(t *testing.T, lines []string) {
	trace := new(Trace)
	err := parseSectionIntro(lines, trace)
	if err == nil {
		t.Fatal("Trace should be invalid:", lines)
	}
	if e, ok := err.(*sectionError); !ok {
		t.Fatal("Should return sectionError. Instead got", e)
	}
}

func TestParseSectionIntro_Invalid(t *testing.T) {
	// Incompleteness
	checkForSectionError(t, []string{})
	checkForSectionError(t, []string{""})
	checkForSectionError(t, []string{"10"})
	checkForSectionError(t, []string{"10", ""})
	checkForSectionError(t, []string{"10", "BUILDID"})
	checkForSectionError(t, []string{"10", "BUILDID", ""})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 789", ""})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 789", "76"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 789", "76 85"})

	// Integer conversion
	checkForSectionError(t, []string{"STR", "BUILDID", "NODEID 123 321 789", "76 85 99"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID STR 321 789", "76 85 99"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 STR 789", "76 85 99"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 STR", "76 85 99"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 789", "STR 85 99"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 789", "76 STR 99"})
	checkForSectionError(t, []string{"10", "BUILDID", "NODEID 123 321 789", "76 85 STR"})
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
	if !proto.Equal(&expectedTrace, &trace) {
		t.Fatalf("Protocol buffers not equal")
	}
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
	if !proto.Equal(&expectedTrace, &trace) {
		t.Fatal("Protocol buffers not equal")
	}
}
