package bismarkpassive

import (
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

func ExampleParseSections_simple() {
    lines := [][]byte {
        []byte("hello"),
        []byte("world"),
        []byte(""),
        []byte("test"),
    }
    printSections(parseSections(lines))

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

func ExampleParseSections_empty() {
    lines := [][]byte {
        []byte("hello"),
        []byte("world"),
        []byte(""),
        []byte(""),
        []byte("test"),
    }
    printSections(parseSections(lines))

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

func ExampleParseSections_trim() {
    lines := [][]byte {
        []byte("hello"),
        []byte("world"),
        []byte(""),
    }
    printSections(parseSections(lines))

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
        t.Fatal("Empty trace should be invalid")
    }
    if e, ok := err.(*sectionError); !ok {
        t.Fatal("Should return sectionError. Instead got", e)
    }
}

func TestParseSectionIntro_Invalid(t *testing.T) {
    checkForSectionError(t, []string {})
    checkForSectionError(t, []string {""})
    checkForSectionError(t, []string {"hello"})
}

func TestParseSectionIntro_Valid(t *testing.T) {
    lines := []string { "10" }
    trace := Trace{}
    err := parseSectionIntro(lines, &trace)
    if err != nil {
        t.Fatal("Should not return error:", err)
    }
    if trace.GetFileFormatVersion() != 10 {
        t.Fatal("Unexpected version", trace.GetFileFormatVersion())
    }
}
