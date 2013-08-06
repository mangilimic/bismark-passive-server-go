package passive

import (
	"regexp"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func LookupsPerDevicePipeline(levelDbManager store.Manager, workers int) transformer.Pipeline {
	tracesStore := levelDbManager.Seeker("traces")
	availabilityIntervalsStore := levelDbManager.Seeker("consistent-ranges")
	addressIdStore := levelDbManager.Seeker("bytesperdomain-address-id-table")
	addressIdToDomainStore := levelDbManager.SeekingWriter("lookupsperdevice-address-id-to-domain")
	lookupsPerDeviceSharded := levelDbManager.ReadingWriter("lookupsperdevice-sharded")
	lookupsPerDeviceStore := levelDbManager.Writer("lookupsperdevice-lookups-per-device")
	lookupsPerDevicePerHourStore := levelDbManager.Writer("lookupsperdevice-lookups-per-device-per-hour")
	consistentTracesStore := store.NewRangeIncludingReader(tracesStore, availabilityIntervalsStore)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "LookupsPerDeviceMapper",
			Reader:      consistentTracesStore,
			Transformer: transformer.MakeDoFunc(lookupsPerDeviceMapper, workers),
			Writer:      addressIdToDomainStore,
		},
		transformer.PipelineStage{
			Name:        "JoinMacWithLookups",
			Reader:      store.NewDemuxingSeeker(addressIdStore, addressIdToDomainStore),
			Transformer: transformer.TransformFunc(joinMacWithLookups),
			Writer:      lookupsPerDeviceSharded,
		},
		transformer.PipelineStage{
			Name:        "FlattenLookupsToNodeAndMac",
			Reader:      lookupsPerDeviceSharded,
			Transformer: transformer.TransformFunc(flattenLookupsToNodeAndMac),
			Writer:      lookupsPerDeviceStore,
		},
		transformer.PipelineStage{
			Name:        "FlattenLookupsToNodeMacAndTimestamp",
			Reader:      lookupsPerDeviceSharded,
			Transformer: transformer.TransformFunc(flattenLookupsToNodeMacAndTimestamp),
			Writer:      lookupsPerDevicePerHourStore,
		},
	}
}

func lookupsPerDeviceMapper(record *store.Record, outputChan chan *store.Record) {
	mobileDomainRegexp, err := regexp.Compile(`(^m\.|\.m\.)`)
	if err != nil {
		panic(err)
	}

	var traceKey TraceKey
	lex.DecodeOrDie(record.Key, &traceKey)
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}

	allDomains := make(map[int32]map[string]int64)
	for _, entry := range trace.ARecord {
		if entry.AddressId == nil || entry.Domain == nil || entry.Anonymized == nil {
			continue
		}
		if *entry.Anonymized {
			continue
		}
		if !mobileDomainRegexp.MatchString(*entry.Domain) {
			continue
		}

		if _, ok := allDomains[*entry.AddressId]; !ok {
			allDomains[*entry.AddressId] = make(map[string]int64)
		}
		allDomains[*entry.AddressId][*entry.Domain]++
	}
	for _, entry := range trace.CnameRecord {
		if entry.AddressId == nil || entry.Domain == nil || entry.DomainAnonymized == nil || entry.Cname == nil || entry.CnameAnonymized == nil {
			continue
		}
		if _, ok := allDomains[*entry.AddressId]; !ok {
			allDomains[*entry.AddressId] = make(map[string]int64)
		}
		if !*entry.DomainAnonymized && mobileDomainRegexp.MatchString(*entry.Domain) {
			allDomains[*entry.AddressId][*entry.Domain]++
		}
		if !*entry.CnameAnonymized && mobileDomainRegexp.MatchString(*entry.Cname) {
			allDomains[*entry.AddressId][*entry.Cname]++
		}
	}

	for addressId, domainsMap := range allDomains {
		for domain, count := range domainsMap {
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(traceKey.SessionKey(), addressId, traceKey.SequenceNumber, domain),
				Value: lex.EncodeOrDie(count),
			}
		}
	}
}

func joinMacWithLookups(inputChan, outputChan chan *store.Record) {
	var (
		session   SessionKey
		addressId int32
	)
	grouper := transformer.GroupRecords(inputChan, &session, &addressId)
	for grouper.NextGroup() {
		var macAddress []byte
		for grouper.NextRecord() {
			record := grouper.Read()
			switch record.DatabaseIndex {
			case 0:
				lex.DecodeOrDie(record.Value, &macAddress)
			case 1:
				if macAddress != nil {
					var (
						sequenceNumber int32
						domain         string
					)
					lex.DecodeOrDie(record.Key, &sequenceNumber, &domain)
					outputChan <- &store.Record{
						Key:   lex.EncodeOrDie(session.NodeId, macAddress, domain, session.AnonymizationContext, session.SessionId, sequenceNumber),
						Value: record.Value,
					}
				}
			}
		}
	}
}

func flattenLookupsToNodeAndMac(inputChan, outputChan chan *store.Record) {
	var nodeId, macAddress, domain string
	grouper := transformer.GroupRecords(inputChan, &nodeId, &macAddress, &domain)
	for grouper.NextGroup() {
		var totalCount int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var count int64
			lex.DecodeOrDie(record.Value, &count)
			totalCount += count
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(nodeId, macAddress, domain),
			Value: lex.EncodeOrDie(totalCount),
		}
	}
}

func flattenLookupsToNodeMacAndTimestamp(inputChan, outputChan chan *store.Record) {
	var nodeId, macAddress, domain string
	grouper := transformer.GroupRecords(inputChan, &nodeId, &macAddress, &domain)
	for grouper.NextGroup() {
		totalCounts := make(map[int64]int64)
		for grouper.NextRecord() {
			record := grouper.Read()
			var (
				anonymizationContext string
				sessionId            int64
				sequenceNumber       int32
			)
			lex.DecodeOrDie(record.Key, &anonymizationContext, &sessionId, &sequenceNumber)
			var count int64
			lex.DecodeOrDie(record.Value, &count)
			timestamp := truncateTimestampToHour(sessionId + convertSecondsToMicroseconds(30)*int64(sequenceNumber))
			totalCounts[timestamp] += count
		}
		for timestamp, totalCount := range totalCounts {
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(nodeId, macAddress, domain, timestamp),
				Value: lex.EncodeOrDie(totalCount),
			}
		}
	}
}
