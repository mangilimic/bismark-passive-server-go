package main

import (
	"flag"
	"fmt"
	"github.com/sburnett/bismark-passive-server-go/passive"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"log"
	"os"
	"time"
)

func getPipelineStages(pipelineName string, workers int) []transformer.PipelineStage {
	switch pipelineName {
	case "availability":
		flagset := flag.NewFlagSet("availability", flag.ExitOnError)
		jsonOutput := flagset.String("json_output", "/dev/null", "Write availiability in JSON format to this file.")
		flagset.Parse(flag.Args()[2:])
		jsonHandle, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error opening JSON output: %v", err)
		}
		return passive.AvailabilityPipeline(jsonHandle, time.Now().Unix(), workers)
	case "bytesperminute":
		return passive.BytesPerMinutePipeline(workers)
	case "filter":
		flagset := flag.NewFlagSet("filter", flag.ExitOnError)
		nodeId := flagset.String("node_id", "OWC43DC7B0AE78", "Retain only data from this router.")
		sessionStartDate := flagset.String("session_start_date", "20120301", "Retain only session starting after this date, in YYYYMMDD format.")
		sessionEndDate := flagset.String("session_end_date", "20120401", "Retain only session starting before this date, in YYYYMMDD format.")
		flagset.Parse(flag.Args()[2:])
		return passive.FilterTracesPipeline(*nodeId, *sessionStartDate, *sessionEndDate, workers)
	case "index":
		return []transformer.PipelineStage{
			transformer.PipelineStage{
				Name:        "ParseTraces",
				Transformer: transformer.MakeMultipleOutputsGroupDoFunc(passive.IndexTarballs, workers),
				InputDbs:    []string{"tarnames", "tarnames-indexed"},
				OutputDbs:   []string{"traces", "tarnames-indexed"},
				OnlyKeys:    true,
			},
		}
	//case "bytesperdevice":
	//	return []passive.PipelineStage{
	//		passive.PipelineStage{
	//			Transformer: passive.TransformerFunc(passive.MapFromTrace),
	//			InputDb: "index.leveldb",
	//			InputTable: "trace_data",
	//			OutputDb: "bytes_per_device.leveldb",
	//		},
	//		passive.PipelineStage{
	//			Transformer: passive.TransformerFunc(passive.JoinMacAndFlowId),
	//			InputDb: "bytes_per_device.leveldb",
	//			InputTable: "ip_to_mac_and_flow",
	//			OutputDb: "bytes_per_device.leveldb",
	//		},
	//		passive.PipelineStage{
	//			Transformer: passive.TransformerFunc(passive.JoinMacAndTimestamp),
	//			InputDb: "bytes_per_device.leveldb",
	//			InputTable: "flow_to_bytes_and_mac",
	//			OutputDb: "bytes_per_device.leveldb",
	//		},
	//		passive.PipelineStage{
	//			Transformer: passive.TransformerFunc(passive.BytesPerDeviceReduce),
	//			InputDb: "bytes_per_device.leveldb",
	//			InputTable: "bytes_per_device_with_nonce",
	//			OutputDb: "bytes_per_device.leveldb",
	//		},
	//	}
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

	stages := getPipelineStages(pipelineName, *workers)
	transformer.RunPipeline(dbRoot, stages, *skipStages)
}
