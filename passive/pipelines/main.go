package main

import (
	"flag"
	"fmt"
	"github.com/sburnett/bismark-passive-server-go/passive"
	"github.com/sburnett/cube"
	"log"
	"os"
)

func getPipelineStages(pipelineName string) []passive.PipelineStage {
	switch pipelineName {
	case "availability":
		flagset := flag.NewFlagSet("availability", flag.ExitOnError)
		jsonOutput := flagset.String("json_output", "/dev/null", "Write availiability in JSON format to this file.")
		flagset.Parse(flag.Args()[2:])

		jsonHandle, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error opening JSON output: %v", err)
		}
		return []passive.PipelineStage{
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.AvailabilityMapper),
				InputDb:     "index.leveldb",
				InputTable:  "traces",
				OutputDb:    "availability.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.AvailabilityReducer),
				InputDb:     "availability.leveldb",
				InputTable:  "availability_with_nonce",
				OutputDb:    "availability.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.AvailabilityJson{jsonHandle},
				InputDb:     "availability.leveldb",
				InputTable:  "availability",
				OutputDb:    "availability.leveldb",
			},
		}
	case "bytesperminute":
		return []passive.PipelineStage{
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.BytesPerMinuteMapper),
				InputDb:     "index.leveldb",
				InputTable:  "traces",
				OutputDb:    "bytes_per_minute.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.BytesPerMinuteReducer),
				InputDb:     "bytes_per_minute.leveldb",
				InputTable:  "bytes_per_minute_mapped",
				OutputDb:    "bytes_per_minute.leveldb",
			},
		}
	case "bytesperdevice":
		return []passive.PipelineStage{
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.MapFromTrace),
				InputDb:     "index.leveldb",
				InputTable:  "trace_data",
				OutputDb:    "bytes_per_device.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.JoinMacAndFlowId),
				InputDb:     "bytes_per_device.leveldb",
				InputTable:  "ip_to_mac_and_flow",
				OutputDb:    "bytes_per_device.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.JoinMacAndTimestamp),
				InputDb:     "bytes_per_device.leveldb",
				InputTable:  "flow_to_bytes_and_mac",
				OutputDb:    "bytes_per_device.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.BytesPerDeviceReduce),
				InputDb:     "bytes_per_device.leveldb",
				InputTable:  "bytes_per_device_with_nonce",
				OutputDb:    "bytes_per_device.leveldb",
			},
		}
	default:
		flag.Usage()
		log.Fatalf("Invalid pipeline.")
	}
	return nil
}

func main() {
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

	stages := getPipelineStages(pipelineName)
	passive.RunPipeline(dbRoot, stages, *skipStages)
}
