package main

import (
	"flag"
	"fmt"
	"github.com/sburnett/bismark-passive-server-go/passive"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"log"
	"os"
	"path/filepath"
	"time"
)

func getPipelineStages(pipelineName, dbRoot string, workers int) []transformer.PipelineStage {
	dbPath := func(filename string) string {
		return filepath.Join(dbRoot, filename)
	}
	switch pipelineName {
	case "availability":
		flagset := flag.NewFlagSet("availability", flag.ExitOnError)
		jsonOutput := flagset.String("json_output", "/dev/null", "Write availiability in JSON format to this file.")
		flagset.Parse(flag.Args()[2:])
		jsonHandle, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error opening JSON output: %v", err)
		}
		return passive.AvailabilityPipeline(
			transformer.NewLevelDbStore(dbPath("traces")),
			transformer.NewLevelDbStore(dbPath("availability-intervals")),
			transformer.NewLevelDbStore(dbPath("availability-consolidated")),
			transformer.NewLevelDbStore(dbPath("availability-nodes")),
			jsonHandle,
			transformer.NewLevelDbStore(dbPath("availability-done")),
			transformer.NewLevelDbStore(dbPath("consistent-ranges")),
			time.Now().Unix(),
			workers)
	case "bytesperdevice":
		return passive.BytesPerDevicePipeline(
			transformer.NewLevelDbStore(dbPath("traces")),
			transformer.NewLevelDbStore(dbPath("consistent-ranges")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-sessions")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-address-table")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-flow-table")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-packets")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-flow-id-to-mac")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-flow-id-to-macs")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-unreduced")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice")),
			passive.NewBytesPerDevicePostgresStore(),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-trace-key-ranges")),
			transformer.NewLevelDbStore(dbPath("bytesperdevice-consolidated-trace-key-ranges")),
			workers)
	case "bytesperminute":
		return passive.BytesPerMinutePipeline(
			transformer.NewLevelDbStore(dbPath("traces")),
			transformer.NewLevelDbStore(dbPath("bytesperminute-mapped")),
			transformer.NewLevelDbStore(dbPath("bytesperminute")),
			transformer.NewLevelDbStore(dbPath("bytesperhour")),
			passive.NewBytesPerHourPostgresStore(),
			transformer.NewLevelDbStore(dbPath("bytesperminute-trace-key-ranges")),
			transformer.NewLevelDbStore(dbPath("bytesperminute-consolidated-trace-key-ranges")),
			workers)
	case "filternode":
		flagset := flag.NewFlagSet("filter", flag.ExitOnError)
		nodeId := flagset.String("node_id", "OWC43DC7B0AE78", "Retain only data from this router.")
		flagset.Parse(flag.Args()[2:])
		tracesStore := transformer.NewLevelDbStore(dbPath("traces"))
		filteredStore := transformer.NewLevelDbStore(dbPath(fmt.Sprintf("filtered-%s", *nodeId)))
		return []transformer.PipelineStage{
			transformer.PipelineStage{
				Name:   "FilterNode",
				Reader: passive.IncludeNodes(tracesStore, *nodeId),
				Writer: filteredStore,
			},
		}
	case "filterdates":
		flagset := flag.NewFlagSet("filter", flag.ExitOnError)
		sessionStartDate := flagset.String("session_start_date", "20120301", "Retain only session starting after this date, in YYYYMMDD format.")
		sessionEndDate := flagset.String("session_end_date", "20120401", "Retain only session starting before this date, in YYYYMMDD format.")
		flagset.Parse(flag.Args()[2:])
		timeFormatString := "20060102"
		sessionStartTime, err := time.Parse(timeFormatString, *sessionStartDate)
		if err != nil {
			panic(fmt.Errorf("Error parsing start date %s: %v", sessionStartDate, err))
		}
		sessionEndTime, err := time.Parse(timeFormatString, *sessionEndDate)
		if err != nil {
			panic(fmt.Errorf("Error parsing end date %s: %v", sessionEndDate, err))
		}
		return passive.FilterSessionsPipeline(
			sessionStartTime.Unix(),
			sessionEndTime.Unix(),
			transformer.NewLevelDbStore(dbPath("traces")),
			transformer.NewLevelDbStore(dbPath("availability-done")),
			transformer.NewLevelDbStore(dbPath(fmt.Sprintf("filtered-%s-%s", *sessionStartDate, *sessionEndDate))))
	case "index":
		tarnamesStore := transformer.NewLevelDbStore(dbPath("tarnames"))
		tarnamesIndexedStore := transformer.NewLevelDbStore(dbPath("tarnames-indexed"))
		tracesStore := transformer.NewLevelDbStore(dbPath("traces"))
		return []transformer.PipelineStage{
			transformer.PipelineStage{
				Name:        "ParseTraces",
				Transformer: transformer.MakeMultipleOutputsGroupDoFunc(passive.IndexTarballs, 2, workers),
				Reader:      transformer.NewDemuxStoreReader(tarnamesStore, tarnamesIndexedStore),
				Writer:      transformer.NewMuxedStoreWriter(tracesStore, tarnamesIndexedStore),
			},
		}
	case "statistics":
		flagset := flag.NewFlagSet("statistics", flag.ExitOnError)
		jsonOutput := flagset.String("json_output", "/dev/null", "Write statistics in JSON format to this file.")
		flagset.Parse(flag.Args()[2:])
		jsonHandle, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error opening JSON output: %v", err)
		}
		return passive.AggregateStatisticsPipeline(
			transformer.NewLevelDbStore(dbPath("traces")),
			transformer.NewLevelDbStore(dbPath("statistics-trace-aggregates")),
			transformer.NewLevelDbStore(dbPath("statistics-node-aggregates")),
			jsonHandle,
			transformer.NewLevelDbStore(dbPath("statistics-trace-key-ranges")),
			transformer.NewLevelDbStore(dbPath("statistics-consolidated-trace-key-ranges")),
			workers)
	default:
		flag.Usage()
		log.Fatalf("Invalid pipeline.")
	}
	return nil
}

func main() {
	workers := flag.Int("workers", 4, "Number of worker threads for mappers.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <pipeline> <db root>:\n", os.Args[0])
		flag.PrintDefaults()
	}
	skipStages := flag.Int("skip_stages", 0, "Skip this many stages at the beginning of the pipeline.")
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		return
	}
	pipelineName := flag.Arg(0)
	dbRoot := flag.Arg(1)

	go cube.Run(fmt.Sprintf("bismark_passive_pipeline_%s", pipelineName))

	stages := getPipelineStages(pipelineName, dbRoot, *workers)
	transformer.RunPipeline(stages, *skipStages)
}
