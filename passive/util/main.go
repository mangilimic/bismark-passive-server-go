package main

import (
	"bytes"
	"encoding/binary"
	"expvar"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/sburnett/cube"
	"log"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
)

func lstables(dbPath string, db *levigo.DB) {
	recordsRead := expvar.NewInt("RecordsRead")
	bytesRead := expvar.NewInt("BytesRead")
	tablesFound := expvar.NewInt("TablesFound")

	readOpts := levigo.NewReadOptions()
	defer readOpts.Close()

	var currentTable []byte
	var currentTableIndex int
	tables := [][]byte{}
	tableRows := make(map[int]int64)
	tableBytes := make(map[int]int64)

	it := db.NewIterator(readOpts)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		recordsRead.Add(1)
		rowBytes := int64(len(it.Key()) + len(it.Value()))
		bytesRead.Add(rowBytes)

		pieces := bytes.SplitAfterN(it.Key(), []byte(":"), 2)
		if len(pieces) == 1 {
			continue
		}
		table := pieces[0]
		if !bytes.Equal(currentTable, table) {
			currentTable = table
			currentTableIndex = len(tables)
			tables = append(tables, table)
			tablesFound.Add(1)
		}
		tableRows[currentTableIndex] += 1
		tableBytes[currentTableIndex] += rowBytes
	}
	if err := it.GetError(); err != nil {
		log.Fatalf("Error iterating through database: %v", err)
	}
	log.Printf("Done iterating tables")

	tw := tabwriter.NewWriter(os.Stdout, 5, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Printf("Table summary for %q:\n", dbPath)
	fmt.Fprintf(tw, "Table\tRows\tBytes\tBytes per row\t\n")
	fmt.Fprintf(tw, "-----\t----\t-----\t-------------\t\n")
	var totalRows int64
	var totalBytes int64
	for idx, table := range tables {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t\n", table[:len(table)-1], tableRows[idx], tableBytes[idx], tableBytes[idx]/tableRows[idx])
		totalRows += tableRows[idx]
		totalBytes += tableBytes[idx]
	}
	fmt.Fprintf(tw, "Total\t%d\t%d\t%d\t\n", totalRows, totalBytes, totalBytes/int64(totalRows))
	tw.Flush()
}

func printtable(dbPath string, db *levigo.DB) {
	flagset := flag.NewFlagSet("printtable", flag.ExitOnError)
	rowLimit := flagset.Int64("limit", -1, "Process at most this many rows. Pass -1 to process all rows.")
	varintValues := flagset.Bool("varint_values", false, "Treat table values as varints and decode appropriately.")
	onlyKeys := flagset.Bool("only_keys", false, "Only print keys.")
	flagset.Parse(flag.Args()[2:])
	if flagset.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "printtable usage: %s <leveldb> printtable [flags] <table>\n", os.Args[0])
		flagset.PrintDefaults()
		log.Fatalf("No table to print")
	}

	table := []byte(flagset.Arg(0))
	tablePrefix := bytes.Join([][]byte{table, []byte(":")}, []byte{})

	var rowsPrinted int64
	recordsRead := expvar.NewInt("RecordsRead")
	bytesRead := expvar.NewInt("BytesRead")

	readOpts := levigo.NewReadOptions()
	defer readOpts.Close()
	it := db.NewIterator(readOpts)
	for it.Seek(tablePrefix); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), tablePrefix) {
			break
		}
		recordsRead.Add(1)
		bytesRead.Add(int64(len(it.Key()) + len(it.Value())))
		fmt.Printf("%s", it.Key())
		if !*onlyKeys {
			if *varintValues {
				value, bytesRead := binary.Varint(it.Value())
				if value == 0 && bytesRead <= 0 {
					log.Fatalf("Error decoding VarInt")
				}
				fmt.Printf(": %d\n", value)
			} else {
				fmt.Printf(": %s\n", it.Value())
			}
		} else {
			fmt.Printf("\n")
		}
		rowsPrinted++
		if *rowLimit >= 0 && rowsPrinted >= *rowLimit {
			log.Printf("Limiting print to %d rows\n", *rowLimit)
			break
		}
	}
	if err := it.GetError(); err != nil {
		log.Fatalf("Error iterating through database: %v", err)
	}
}

func rmtables(dbPath string, db *levigo.DB) {
	recordsRemoved := expvar.NewInt("RecordsRemoved")
	bytesRemoved := expvar.NewInt("BytesRemoved")

	flagset := flag.NewFlagSet("rmtables", flag.ExitOnError)
	flagset.Parse(flag.Args()[2:])
	if flagset.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "rmtables usage: %s <leveldb> rmtables <table1> <table2> ... <tableN>\n", os.Args[0])
		flagset.PrintDefaults()
		log.Fatalf("No table to remove")
	}

	for _, arg := range flagset.Args() {
		table := []byte(arg)
		tablePrefix := bytes.Join([][]byte{table, []byte(":")}, []byte{})
		var rowsRemoved int64

		readOpts := levigo.NewReadOptions()
		defer readOpts.Close()
		writeOpts := levigo.NewWriteOptions()
		defer writeOpts.Close()
		it := db.NewIterator(readOpts)
		for it.Seek(tablePrefix); it.Valid(); it.Next() {
			if !bytes.HasPrefix(it.Key(), tablePrefix) {
				break
			}
			recordsRemoved.Add(1)
			rowsRemoved++
			bytesRemoved.Add(int64(len(it.Key()) + len(it.Value())))
			if err := db.Delete(writeOpts, it.Key()); err != nil {
				log.Fatalf("Error removing key %s: %v", it.Key(), err)
			}
		}
		if err := it.GetError(); err != nil {
			log.Fatalf("Error iterating through database: %v", err)
		}
		log.Printf("Removed %d rows from table %s", rowsRemoved, table)
	}
	log.Printf("Done. Removed %s rows in total", recordsRemoved)
}

func main() {
	operations := map[string]func(string, *levigo.DB){
		"lstables":   lstables,
		"printtable": printtable,
		"rmtables":   rmtables,
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <leveldb> <operation> ...:\n", os.Args[0])
		operationNames := []string{}
		for name, _ := range operations {
			operationNames = append(operationNames, name)
		}
		sort.Sort(sort.StringSlice(operationNames))
		fmt.Fprintf(os.Stderr, "Available operations: %v\n", strings.Join(operationNames, ", "))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		return
	}
	dbPath := flag.Arg(0)
	operation := flag.Arg(1)

	go cube.Run(fmt.Sprintf("bismark_passive_util_%s", operation))

	opts := levigo.NewOptions()
	opts.SetMaxOpenFiles(128)
	opts.SetCreateIfMissing(false)
	defer opts.Close()
	db, err := levigo.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Error opening leveldb database %v: %v", dbPath, err)
	}
	defer db.Close()

	operationFunc, ok := operations[operation]
	if !ok {
		flag.Usage()
		log.Fatalf("Invalid operation")
	}
	operationFunc(dbPath, db)
}
