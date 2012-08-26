package bismarkpassive

import (
    "fmt"
)

func printSections(sections [][][]byte) {
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
