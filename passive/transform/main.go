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
	transformers := map[string]passive.Transformer{
		"availability_map":                        passive.TransformerFunc(passive.AvailabilityMapper),
		"bytes_per_minute_map":                    passive.TransformerFunc(passive.BytesPerMinuteMapper),
		"bytes_per_minute_reduce":                 passive.TransformerFunc(passive.BytesPerMinuteReducer),
		"bytes_per_device_map_from_trace":         passive.TransformerFunc(passive.MapFromTrace),
		"bytes_per_device_join_mac_and_flow_id":   passive.TransformerFunc(passive.JoinMacAndFlowId),
		"bytes_per_device_join_mac_and_timestamp": passive.TransformerFunc(passive.JoinMacAndTimestamp),
		"bytes_per_device_reduce":                 passive.TransformerFunc(passive.BytesPerDeviceReduce),
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <transform> <input leveldb> <input table> <output leveldb>:\n", os.Args[0])
		transformerNames := []string{}
		for name, _ := range transformers {
			transformerNames = append(transformerNames, name)
		}
		fmt.Fprintf(os.Stderr, "Available transforms: %v\n", strings.Join(transformerNames, ", "))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 4 {
		flag.Usage()
		return
	}
	transform := flag.Arg(0)
	inputDbPath := flag.Arg(1)
	inputTable := flag.Arg(2)
	outputDbPath := flag.Arg(3)

	go cube.Run(fmt.Sprintf("bismark_passive_transform_%s", transform))

	transformer, ok := transformers[transform]
	if !ok {
		flag.Usage()
		log.Fatalf("Invalid transform.")
	}
	passive.RunTransformer(transformer, inputDbPath, inputTable, outputDbPath)
}
