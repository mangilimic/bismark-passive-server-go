package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sburnett/bismark-passive-server-go/passive"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func pipelineAvailability() transformer.Pipeline {
	flagset := flag.NewFlagSet("availability", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	jsonOutput := flagset.String("json_output", "/dev/null", "Write availability in JSON format to this file.")
	flagset.Parse(flag.Args()[1:])
	jsonHandle, err := os.Create(*jsonOutput)
	if err != nil {
		log.Fatalf("Error opening JSON output: %v", err)
	}
	return passive.AvailabilityPipeline(store.NewLevelDbManager(*dbRoot), jsonHandle, time.Now().Unix())
}

func pipelineBytesPerDevice() transformer.Pipeline {
	flagset := flag.NewFlagSet("bytesperdevice", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return passive.BytesPerDevicePipeline(store.NewLevelDbManager(*dbRoot), passive.NewBytesPerDevicePostgresStore())
}

func pipelineBytesPerDomain() transformer.Pipeline {
	flagset := flag.NewFlagSet("bytesperdomain", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return passive.BytesPerDomainPipeline(store.NewLevelDbManager(*dbRoot), passive.NewBytesPerDomainPostgresStore())
}

func pipelineBytesPerMinute() transformer.Pipeline {
	flagset := flag.NewFlagSet("bytesperminute", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return passive.BytesPerMinutePipeline(store.NewLevelDbManager(*dbRoot), passive.NewBytesPerHourPostgresStore())
}

func pipelineFilterNode() transformer.Pipeline {
	flagset := flag.NewFlagSet("filter", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	nodeId := flagset.String("node_id", "OWC43DC7B0AE78", "Retain only data from this router.")
	flagset.Parse(flag.Args()[1:])
	return passive.FilterNodesPipeline(*nodeId, store.NewLevelDbManager(*dbRoot))
}

func pipelineFilterDates() transformer.Pipeline {
	flagset := flag.NewFlagSet("filter", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	sessionStartDate := flagset.String("session_start_date", "20120301", "Retain only session starting after this date, in YYYYMMDD format.")
	sessionEndDate := flagset.String("session_end_date", "20120401", "Retain only session starting before this date, in YYYYMMDD format.")
	flagset.Parse(flag.Args()[1:])
	timeFormatString := "20060102"
	sessionStartTime, err := time.Parse(timeFormatString, *sessionStartDate)
	if err != nil {
		panic(fmt.Errorf("Error parsing start date %s: %v", sessionStartDate, err))
	}
	sessionEndTime, err := time.Parse(timeFormatString, *sessionEndDate)
	if err != nil {
		panic(fmt.Errorf("Error parsing end date %s: %v", sessionEndDate, err))
	}
	outputName := fmt.Sprintf("filtered-%s-%s", *sessionStartDate, *sessionEndDate)
	return passive.FilterSessionsPipeline(sessionStartTime.Unix(), sessionEndTime.Unix(), store.NewLevelDbManager(*dbRoot), outputName)
}

func pipelineIndex() transformer.Pipeline {
	flagset := flag.NewFlagSet("index", flag.ExitOnError)
	tarballsPath := flagset.String("tarballs_path", "/data/users/sburnett/passive-organized", "Read tarballs from this directory.")
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return passive.IndexTarballsPipeline(*tarballsPath, store.NewLevelDbManager(*dbRoot))
}

func pipelineLookupsPerDevice() transformer.Pipeline {
	flagset := flag.NewFlagSet("lookupsperdevice", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return passive.LookupsPerDevicePipeline(store.NewLevelDbManager(*dbRoot))
}

func pipelineStatistics() transformer.Pipeline {
	flagset := flag.NewFlagSet("statistics", flag.ExitOnError)
	dbRoot := flagset.String("passive_leveldb_root", "/data/users/sburnett/passive-leveldb-new", "Write leveldbs in this directory.")
	jsonOutput := flagset.String("json_output", "/dev/null", "Write statistics in JSON format to this file.")
	flagset.Parse(flag.Args()[2:])
	jsonHandle, err := os.Create(*jsonOutput)
	if err != nil {
		log.Fatalf("Error opening JSON output: %v", err)
	}
	return passive.AggregateStatisticsPipeline(store.NewLevelDbManager(*dbRoot), jsonHandle)
}

func main() {
	pipelineFuncs := map[string]transformer.PipelineThunk{
		"availability":     pipelineAvailability,
		"bytesperdevice":   pipelineBytesPerDevice,
		"bytesperdomain":   pipelineBytesPerDomain,
		"bytesperminute":   pipelineBytesPerMinute,
		"filternode":       pipelineFilterNode,
		"filterdates":      pipelineFilterDates,
		"index":            pipelineIndex,
		"lookupsperdevice": pipelineLookupsPerDevice,
		"statistics":       pipelineStatistics,
	}
	name, pipeline := transformer.ParsePipelineChoice(pipelineFuncs)

	go cube.Run(fmt.Sprintf("bismark_passive_pipeline_%s", name))

	transformer.RunPipeline(pipeline)
}
