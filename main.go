package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/sburnett/bismark-passive-server-go/passive"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func pipelineAvailability(dbRoot string, workers int) transformer.Pipeline {
	flagset := flag.NewFlagSet("availability", flag.ExitOnError)
	jsonOutput := flagset.String("json_output", "/dev/null", "Write availiability in JSON format to this file.")
	flagset.Parse(flag.Args()[2:])
	jsonHandle, err := os.Create(*jsonOutput)
	if err != nil {
		log.Fatalf("Error opening JSON output: %v", err)
	}
	return passive.AvailabilityPipeline(
		store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "availability-intervals")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "availability-consolidated")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "availability-nodes")),
		jsonHandle,
		store.NewLevelDbStore(filepath.Join(dbRoot, "availability-done")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "consistent-ranges")),
		time.Now().Unix(),
		workers)
}

func pipelineBytesPerDevice(dbRoot string, workers int) transformer.Pipeline {
	return passive.BytesPerDevicePipeline(
		store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "consistent-ranges")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-sessions")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-address-table")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-flow-table")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-packets")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-flow-id-to-mac")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-flow-id-to-macs")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-unreduced")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-reduced-sessions")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice")),
		passive.NewBytesPerDevicePostgresStore(),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-trace-key-ranges")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdevice-consolidated-trace-key-ranges")),
		workers)
}

func pipelineBytesPerDomain(dbRoot string, workers int) transformer.Pipeline {
	stores := passive.BytesPerDomainPipelineStores{
		Traces:                     store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		AvailabilityIntervals:      store.NewLevelDbStore(filepath.Join(dbRoot, "consistent-ranges")),
		TraceKeyRanges:             store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-trace-key-ranges")),
		ConsolidatedTraceKeyRanges: store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-consolidated-trace-key-ranges")),
		AddressIdTable:             store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-address-id-table")),
		ARecordTable:               store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-a-record-table")),
		CnameRecordTable:           store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-cname-record-table")),
		FlowIpsTable:               store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-flow-ips-table")),
		AddressIpTable:             store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-address-ip-table")),
		BytesPerTimestampSharded:   store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-bytes-per-timestamp-sharded")),
		Whitelist:                  store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-whitelist")),
		ARecordsWithMac:            store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-a-records-with-mac")),
		CnameRecordsWithMac:        store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-cname-records-with-mac")),
		AllDnsMappings:             store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-all-dns-mappings")),
		AllWhitelistedMappings:     store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-all-whitelisted-mappings")),
		FlowMacsTable:              store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-flow-macs-table")),
		FlowDomainsTable:           store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-flow-domains-table")),
		FlowDomainsGroupedTable:    store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-flow-domains-grouped-table")),
		BytesPerDomainSharded:      store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-bytes-per-domain-sharded")),
		BytesPerDomainPerDevice:    store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-bytes-per-domain-per-device")),
		BytesPerDomain:             store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-bytes-per-domain")),
		BytesPerDomainPostgres:     passive.NewBytesPerDomainPostgresStore(),
		Sessions:                   store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-sessions")),
	}
	return passive.BytesPerDomainPipeline(&stores, workers)
}

func pipelineBytesPerMinute(dbRoot string, workers int) transformer.Pipeline {
	return passive.BytesPerMinutePipeline(
		store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperminute-mapped")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperminute")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperhour")),
		passive.NewBytesPerHourPostgresStore(),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperminute-trace-key-ranges")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperminute-consolidated-trace-key-ranges")),
		workers)
}

func pipelineFilterNode(dbRoot string, workers int) transformer.Pipeline {
	flagset := flag.NewFlagSet("filter", flag.ExitOnError)
	nodeId := flagset.String("node_id", "OWC43DC7B0AE78", "Retain only data from this router.")
	flagset.Parse(flag.Args()[2:])
	tracesStore := store.NewLevelDbStore(filepath.Join(dbRoot, "traces"))
	filteredStore := store.NewLevelDbStore(filepath.Join(dbRoot, fmt.Sprintf("filtered-%s", *nodeId)))
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:   "FilterNode",
			Reader: passive.IncludeNodes(tracesStore, *nodeId),
			Writer: filteredStore,
		},
	}
}

func pipelineFilterDates(dbRoot string, workers int) transformer.Pipeline {
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
		store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "availability-done")),
		store.NewLevelDbStore(filepath.Join(dbRoot, fmt.Sprintf("filtered-%s-%s", *sessionStartDate, *sessionEndDate))))
}

func pipelineIndex(dbRoot string, workers int) transformer.Pipeline {
	flagset := flag.NewFlagSet("index", flag.ExitOnError)
	tarballsPath := flagset.String("tarballs_path", "/data/users/sburnett/passive-organized", "Read tarballs from this directory.")
	flagset.Parse(flag.Args()[2:])
	tarnamesStore := store.NewLevelDbStore(filepath.Join(dbRoot, "tarnames"))
	tarnamesIndexedStore := store.NewLevelDbStore(filepath.Join(dbRoot, "tarnames-indexed"))
	tracesStore := store.NewLevelDbStore(filepath.Join(dbRoot, "traces"))
	return passive.IndexTarballsPipeline(*tarballsPath, tarnamesStore, tarnamesIndexedStore, tracesStore, workers)
}

func pipelineLookupsPerDevice(dbRoot string, workers int) transformer.Pipeline {
	return passive.LookupsPerDevicePipeline(
		store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "consistent-ranges")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "bytesperdomain-address-id-table")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "lookupsperdevice-address-id-to-domain")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "lookupsperdevice-sharded")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "lookupsperdevice-lookups-per-device")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "lookupsperdevice-lookups-per-device-per-hour")),
		workers)
}

func pipelineStatistics(dbRoot string, workers int) transformer.Pipeline {
	flagset := flag.NewFlagSet("statistics", flag.ExitOnError)
	jsonOutput := flagset.String("json_output", "/dev/null", "Write statistics in JSON format to this file.")
	flagset.Parse(flag.Args()[2:])
	jsonHandle, err := os.Create(*jsonOutput)
	if err != nil {
		log.Fatalf("Error opening JSON output: %v", err)
	}
	return passive.AggregateStatisticsPipeline(
		store.NewLevelDbStore(filepath.Join(dbRoot, "traces")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "consistent-ranges")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "statistics-trace-aggregates")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "statistics-session-aggregates")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "statistics-node-aggregates")),
		jsonHandle,
		store.NewLevelDbStore(filepath.Join(dbRoot, "statistics-sessions")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "statistics-trace-key-ranges")),
		store.NewLevelDbStore(filepath.Join(dbRoot, "statistics-consolidated-trace-key-ranges")),
		workers)
}

func main() {
	pipelineFuncs := map[string]transformer.PipelineFunc{
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
