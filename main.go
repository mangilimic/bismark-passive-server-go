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

func getPipelineStages(dbRoot string, workers int) map[string]func() []transformer.PipelineStage {
	dbPath := func(filename string) string {
		return filepath.Join(dbRoot, filename)
	}
	pipelineFuncs := make(map[string]func() []transformer.PipelineStage)
	pipelineFuncs["availability"] = func() []transformer.PipelineStage {
		flagset := flag.NewFlagSet("availability", flag.ExitOnError)
		jsonOutput := flagset.String("json_output", "/dev/null", "Write availiability in JSON format to this file.")
		flagset.Parse(flag.Args()[2:])
		jsonHandle, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error opening JSON output: %v", err)
		}
		return passive.AvailabilityPipeline(
			store.NewLevelDbStore(dbPath("traces")),
			store.NewLevelDbStore(dbPath("availability-intervals")),
			store.NewLevelDbStore(dbPath("availability-consolidated")),
			store.NewLevelDbStore(dbPath("availability-nodes")),
			jsonHandle,
			store.NewLevelDbStore(dbPath("availability-done")),
			store.NewLevelDbStore(dbPath("consistent-ranges")),
			time.Now().Unix(),
			workers)
	}
	pipelineFuncs["bytesperdevice"] = func() []transformer.PipelineStage {
		return passive.BytesPerDevicePipeline(
			store.NewLevelDbStore(dbPath("traces")),
			store.NewLevelDbStore(dbPath("consistent-ranges")),
			store.NewLevelDbStore(dbPath("bytesperdevice-sessions")),
			store.NewLevelDbStore(dbPath("bytesperdevice-address-table")),
			store.NewLevelDbStore(dbPath("bytesperdevice-flow-table")),
			store.NewLevelDbStore(dbPath("bytesperdevice-packets")),
			store.NewLevelDbStore(dbPath("bytesperdevice-flow-id-to-mac")),
			store.NewLevelDbStore(dbPath("bytesperdevice-flow-id-to-macs")),
			store.NewLevelDbStore(dbPath("bytesperdevice-unreduced")),
			store.NewLevelDbStore(dbPath("bytesperdevice-reduced-sessions")),
			store.NewLevelDbStore(dbPath("bytesperdevice")),
			passive.NewBytesPerDevicePostgresStore(),
			store.NewLevelDbStore(dbPath("bytesperdevice-trace-key-ranges")),
			store.NewLevelDbStore(dbPath("bytesperdevice-consolidated-trace-key-ranges")),
			workers)
	}
	pipelineFuncs["bytesperdomain"] = func() []transformer.PipelineStage {
		stores := passive.BytesPerDomainPipelineStores{
			Traces:                     store.NewLevelDbStore(dbPath("traces")),
			AvailabilityIntervals:      store.NewLevelDbStore(dbPath("consistent-ranges")),
			TraceKeyRanges:             store.NewLevelDbStore(dbPath("bytesperdomain-trace-key-ranges")),
			ConsolidatedTraceKeyRanges: store.NewLevelDbStore(dbPath("bytesperdomain-consolidated-trace-key-ranges")),
			AddressIdTable:             store.NewLevelDbStore(dbPath("bytesperdomain-address-id-table")),
			ARecordTable:               store.NewLevelDbStore(dbPath("bytesperdomain-a-record-table")),
			CnameRecordTable:           store.NewLevelDbStore(dbPath("bytesperdomain-cname-record-table")),
			FlowIpsTable:               store.NewLevelDbStore(dbPath("bytesperdomain-flow-ips-table")),
			AddressIpTable:             store.NewLevelDbStore(dbPath("bytesperdomain-address-ip-table")),
			BytesPerTimestampSharded:   store.NewLevelDbStore(dbPath("bytesperdomain-bytes-per-timestamp-sharded")),
			Whitelist:                  store.NewLevelDbStore(dbPath("bytesperdomain-whitelist")),
			ARecordsWithMac:            store.NewLevelDbStore(dbPath("bytesperdomain-a-records-with-mac")),
			CnameRecordsWithMac:        store.NewLevelDbStore(dbPath("bytesperdomain-cname-records-with-mac")),
			AllDnsMappings:             store.NewLevelDbStore(dbPath("bytesperdomain-all-dns-mappings")),
			AllWhitelistedMappings:     store.NewLevelDbStore(dbPath("bytesperdomain-all-whitelisted-mappings")),
			FlowMacsTable:              store.NewLevelDbStore(dbPath("bytesperdomain-flow-macs-table")),
			FlowDomainsTable:           store.NewLevelDbStore(dbPath("bytesperdomain-flow-domains-table")),
			FlowDomainsGroupedTable:    store.NewLevelDbStore(dbPath("bytesperdomain-flow-domains-grouped-table")),
			BytesPerDomainSharded:      store.NewLevelDbStore(dbPath("bytesperdomain-bytes-per-domain-sharded")),
			BytesPerDomainPerDevice:    store.NewLevelDbStore(dbPath("bytesperdomain-bytes-per-domain-per-device")),
			BytesPerDomain:             store.NewLevelDbStore(dbPath("bytesperdomain-bytes-per-domain")),
			BytesPerDomainPostgres:     passive.NewBytesPerDomainPostgresStore(),
			Sessions:                   store.NewLevelDbStore(dbPath("bytesperdomain-sessions")),
		}
		return passive.BytesPerDomainPipeline(&stores, workers)
	}
	pipelineFuncs["bytesperminute"] = func() []transformer.PipelineStage {
		return passive.BytesPerMinutePipeline(
			store.NewLevelDbStore(dbPath("traces")),
			store.NewLevelDbStore(dbPath("bytesperminute-mapped")),
			store.NewLevelDbStore(dbPath("bytesperminute")),
			store.NewLevelDbStore(dbPath("bytesperhour")),
			passive.NewBytesPerHourPostgresStore(),
			store.NewLevelDbStore(dbPath("bytesperminute-trace-key-ranges")),
			store.NewLevelDbStore(dbPath("bytesperminute-consolidated-trace-key-ranges")),
			workers)
	}
	pipelineFuncs["filternode"] = func() []transformer.PipelineStage {
		flagset := flag.NewFlagSet("filter", flag.ExitOnError)
		nodeId := flagset.String("node_id", "OWC43DC7B0AE78", "Retain only data from this router.")
		flagset.Parse(flag.Args()[2:])
		tracesStore := store.NewLevelDbStore(dbPath("traces"))
		filteredStore := store.NewLevelDbStore(dbPath(fmt.Sprintf("filtered-%s", *nodeId)))
		return []transformer.PipelineStage{
			transformer.PipelineStage{
				Name:   "FilterNode",
				Reader: passive.IncludeNodes(tracesStore, *nodeId),
				Writer: filteredStore,
			},
		}
	}
	pipelineFuncs["filterdates"] = func() []transformer.PipelineStage {
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
			store.NewLevelDbStore(dbPath("traces")),
			store.NewLevelDbStore(dbPath("availability-done")),
			store.NewLevelDbStore(dbPath(fmt.Sprintf("filtered-%s-%s", *sessionStartDate, *sessionEndDate))))
	}
	pipelineFuncs["index"] = func() []transformer.PipelineStage {
		flagset := flag.NewFlagSet("index", flag.ExitOnError)
		tarballsPath := flagset.String("tarballs_path", "/data/users/sburnett/passive-organized", "Read tarballs from this directory.")
		flagset.Parse(flag.Args()[2:])
		tarnamesStore := store.NewLevelDbStore(dbPath("tarnames"))
		tarnamesIndexedStore := store.NewLevelDbStore(dbPath("tarnames-indexed"))
		tracesStore := store.NewLevelDbStore(dbPath("traces"))
		return passive.IndexTarballsPipeline(*tarballsPath, tarnamesStore, tarnamesIndexedStore, tracesStore, workers)
	}
	pipelineFuncs["lookupsperdevice"] = func() []transformer.PipelineStage {
		return passive.LookupsPerDevicePipeline(
			store.NewLevelDbStore(dbPath("traces")),
			store.NewLevelDbStore(dbPath("consistent-ranges")),
			store.NewLevelDbStore(dbPath("bytesperdomain-address-id-table")),
			store.NewLevelDbStore(dbPath("lookupsperdevice-address-id-to-domain")),
			store.NewLevelDbStore(dbPath("lookupsperdevice-sharded")),
			store.NewLevelDbStore(dbPath("lookupsperdevice-lookups-per-device")),
			store.NewLevelDbStore(dbPath("lookupsperdevice-lookups-per-device-per-hour")),
			workers)
	}
	pipelineFuncs["statistics"] = func() []transformer.PipelineStage {
		flagset := flag.NewFlagSet("statistics", flag.ExitOnError)
		jsonOutput := flagset.String("json_output", "/dev/null", "Write statistics in JSON format to this file.")
		flagset.Parse(flag.Args()[2:])
		jsonHandle, err := os.Create(*jsonOutput)
		if err != nil {
			log.Fatalf("Error opening JSON output: %v", err)
		}
		return passive.AggregateStatisticsPipeline(
			store.NewLevelDbStore(dbPath("traces")),
			store.NewLevelDbStore(dbPath("consistent-ranges")),
			store.NewLevelDbStore(dbPath("statistics-trace-aggregates")),
			store.NewLevelDbStore(dbPath("statistics-session-aggregates")),
			store.NewLevelDbStore(dbPath("statistics-node-aggregates")),
			jsonHandle,
			store.NewLevelDbStore(dbPath("statistics-sessions")),
			store.NewLevelDbStore(dbPath("statistics-trace-key-ranges")),
			store.NewLevelDbStore(dbPath("statistics-consolidated-trace-key-ranges")),
			workers)
	}
	return pipelineFuncs
}

func main() {
	name, pipeline := transformer.ParsePipelineChoice(getPipelineStages)

	go cube.Run(fmt.Sprintf("bismark_passive_pipeline_%s", name))

	transformer.RunPipeline(pipeline)
}
