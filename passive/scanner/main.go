package main

import (
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer/key"
	"log"
	"os"
	"path/filepath"
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
	tarsPath := flag.Arg(0)
	indexPath := flag.Arg(1)

	go cube.Run("bismark_passive_index")

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(4 * 1024 * 1024)
	defer opts.Close()
	db, err := levigo.Open(filepath.Join(indexPath, "tarnames"), opts)
	if err != nil {
		panic(fmt.Errorf("Error opening database: %v", err))
	}
	defer db.Close()
	writeOpts := levigo.NewWriteOptions()
	defer writeOpts.Close()

	log.Printf("Scanning tarballs.")
	tarFiles, err := filepath.Glob(filepath.Join(tarsPath, "*", "*", "*.tar.gz"))
	if err != nil {
		panic(fmt.Errorf("Error enumerating tarballs: ", err))
	}
	log.Printf("Found %d tarballs.", len(tarFiles))
	for _, tarFile := range tarFiles {
		absTarFile, err := filepath.Abs(tarFile)
		if err != nil {
			panic(fmt.Errorf("Error getting absolute path for %v: %v", tarFile, err))
		}
		db.Put(writeOpts, key.EncodeOrDie(absTarFile), []byte{})
	}
}
