package main

import (
	"bismark/passive"
	"flag"
	"fmt"
	"github.com/sburnett/cube"
	"log"
	"os"
	"strings"
)

func main() {
	pipelines := map[string][]passive.PipelineStage{
		"bytesperminute": []passive.PipelineStage{
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.BytesPerMinuteMapper),
				InputDb: "index.leveldb",
				InputTable: "trace_data",
				OutputDb: "bytes_per_minute.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.BytesPerMinuteReducer),
				InputDb: "bytes_per_minute.leveldb",
				InputTable: "bytes_per_minute_mapped",
				OutputDb: "bytes_per_minute.leveldb",
			},
		},
		"bytesperdevice": []passive.PipelineStage{
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.MapFromTrace),
				InputDb: "index.leveldb",
				InputTable: "trace_data",
				OutputDb: "bytes_per_device.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.JoinMacAndFlowId),
				InputDb: "bytes_per_device.leveldb",
				InputTable: "ip_to_mac_and_flow",
				OutputDb: "bytes_per_device.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.JoinMacAndTimestamp),
				InputDb: "bytes_per_device.leveldb",
				InputTable: "flow_to_bytes_and_mac",
				OutputDb: "bytes_per_device.leveldb",
			},
			passive.PipelineStage{
				Transformer: passive.TransformerFunc(passive.BytesPerDeviceReduce),
				InputDb: "bytes_per_device.leveldb",
				InputTable: "bytes_per_device_with_nonce",
				OutputDb: "bytes_per_device.leveldb",
			},
		},
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <pipeline> <db root>:\n", os.Args[0])
		pipelineNames := []string{}
		for name, _ := range pipelines {
			pipelineNames = append(pipelineNames, name)
		}
		fmt.Fprintf(os.Stderr, "Available pipelines: %v\n", strings.Join(pipelineNames, ", "))
		flag.PrintDefaults()
	}
	skipStages := flag.Int("skip_stages", 0, "Skip this many stages at the beginning of the pipeline.")
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		return
	}
	pipelineName := flag.Arg(0)
	dbRoot := flag.Arg(1)

	go cube.Run(fmt.Sprintf("bismark_passive_pipeline_%s", pipelineName))

	stages, ok := pipelines[pipelineName]
	if !ok {
		flag.Usage()
		log.Fatalf("Invalid pipeline.")
	}
	passive.RunPipeline(dbRoot, stages, *skipStages)
}
