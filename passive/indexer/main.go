package main

import (
	"flag"
	"fmt"
	"github.com/sburnett/bismark-passive-server-go/passive"
	"github.com/sburnett/cube"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <tarballs directory> <leveldb> [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		return
	}

	go cube.Run("bismark_passive_index")

	passive.IndexTraces(flag.Arg(0), flag.Arg(1))
}
