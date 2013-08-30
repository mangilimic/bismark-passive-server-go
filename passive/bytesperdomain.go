package passive

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"code.google.com/p/goprotobuf/proto"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func BytesPerDomainPipeline(levelDbManager store.Manager, bytesPerDomainPostgresStore store.Writer) transformer.Pipeline {
	tracesStore := levelDbManager.Seeker("traces")
	availabilityIntervalsStore := levelDbManager.Seeker("consistent-ranges")
	traceKeyRangesStore := levelDbManager.ReadingDeleter("bytesperdomain-trace-key-ranges")
	consolidatedTraceKeyRangesStore := levelDbManager.ReadingDeleter("bytesperdomain-consolidated-trace-key-ranges")
	addressIdTableStore := levelDbManager.SeekingWriter("bytesperdomain-address-id-table")
	aRecordTableStore := levelDbManager.SeekingWriter("bytesperdomain-a-record-table")
	cnameRecordTableStore := levelDbManager.SeekingWriter("bytesperdomain-cname-record-table")
	flowIpsTableStore := levelDbManager.SeekingWriter("bytesperdomain-flow-ips-table")
	addressIpTableStore := levelDbManager.SeekingWriter("bytesperdomain-address-ip-table")
	bytesPerTimestampShardedStore := levelDbManager.SeekingWriter("bytesperdomain-bytes-per-timestamp-sharded")
	whitelistStore := levelDbManager.SeekingWriter("bytesperdomain-whitelist")
	aRecordsWithMacStore := levelDbManager.SeekingWriter("bytesperdomain-a-records-with-mac")
	cnameRecordsWithMacStore := levelDbManager.SeekingWriter("bytesperdomain-cname-records-with-mac")
	allDnsMappingsStore := levelDbManager.SeekingWriter("bytesperdomain-all-dns-mappings")
	allWhitelistedMappingsStore := levelDbManager.SeekingWriter("bytesperdomain-all-whitelisted-mappings")
	flowMacsTableStore := levelDbManager.SeekingWriter("bytesperdomain-flow-macs-table")
	flowDomainsTableStore := levelDbManager.SeekingWriter("bytesperdomain-flow-domains-table")
	flowDomainsGroupedTableStore := levelDbManager.SeekingWriter("bytesperdomain-flow-domains-grouped-table")
	bytesPerDomainShardedStore := levelDbManager.ReadingWriter("bytesperdomain-bytes-per-domain-sharded")
	bytesPerDomainPerDeviceStore := levelDbManager.ReadingWriter("bytesperdomain-bytes-per-domain-per-device")
	bytesPerDomainStore := levelDbManager.ReadingWriter("bytesperdomain-bytes-per-domain")
	sessionsStore := levelDbManager.ReadingDeleter("bytesperdomain-sessions")
	excludeOldSessions := func(stor store.Seeker) store.Seeker {
		return store.NewPrefixIncludingReader(stor, sessionsStore)
	}
	newTracesStore := store.NewRangeExcludingReader(store.NewRangeIncludingReader(tracesStore, availabilityIntervalsStore), traceKeyRangesStore)
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerDomainMapper",
			Reader:      newTracesStore,
			Transformer: transformer.MakeMultipleOutputsDoFunc(bytesPerDomainMapper, 7),
			Writer:      store.NewMuxingWriter(addressIdTableStore, aRecordTableStore, cnameRecordTableStore, flowIpsTableStore, addressIpTableStore, bytesPerTimestampShardedStore, whitelistStore),
		},
		SessionPipelineStage(newTracesStore, sessionsStore),
		transformer.PipelineStage{
			Name:        "JoinAAddressIdsWithMacAddresses",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(addressIdTableStore, aRecordTableStore)),
			Transformer: transformer.TransformFunc(joinAddressIdsWithMacAddresses),
			Writer:      aRecordsWithMacStore,
		},
		transformer.PipelineStage{
			Name:        "JoinCnameAddressIdsWithMacAddresses",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(addressIdTableStore, cnameRecordTableStore)),
			Transformer: transformer.TransformFunc(joinAddressIdsWithMacAddresses),
			Writer:      cnameRecordsWithMacStore,
		},
		transformer.PipelineStage{
			Name:        "JoinARecordsWithCnameRecords",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(aRecordsWithMacStore, cnameRecordsWithMacStore)),
			Transformer: transformer.TransformFunc(joinARecordsWithCnameRecords),
			Writer:      allDnsMappingsStore,
		},
		transformer.PipelineStage{
			Name:        "EmitARecords",
			Reader:      excludeOldSessions(aRecordsWithMacStore),
			Transformer: transformer.MakeDoFunc(emitARecords),
			Writer:      allDnsMappingsStore,
		},
		transformer.PipelineStage{
			Name:        "JoinDomainsWithWhitelist",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(whitelistStore, allDnsMappingsStore)),
			Transformer: transformer.TransformFunc(joinDomainsWithWhitelist),
			Writer:      allWhitelistedMappingsStore,
		},
		transformer.PipelineStage{
			Name:        "JoinMacWithFlowId",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(addressIpTableStore, flowIpsTableStore)),
			Transformer: transformer.TransformFunc(joinMacWithFlowId),
			Writer:      flowMacsTableStore,
		},
		transformer.PipelineStage{
			Name:        "JoinWhitelistedDomainsWithFlows",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(allWhitelistedMappingsStore, flowMacsTableStore)),
			Transformer: transformer.TransformFunc(joinWhitelistedDomainsWithFlows),
			Writer:      flowDomainsTableStore,
		},
		transformer.PipelineStage{
			Name:        "GroupDomainsAndMacAddresses",
			Reader:      excludeOldSessions(flowDomainsTableStore),
			Transformer: transformer.TransformFunc(groupDomainsAndMacAddresses),
			Writer:      flowDomainsGroupedTableStore,
		},
		transformer.PipelineStage{
			Name:        "JoinDomainsWithSizes",
			Reader:      excludeOldSessions(store.NewDemuxingSeeker(flowDomainsGroupedTableStore, bytesPerTimestampShardedStore)),
			Transformer: transformer.TransformFunc(joinDomainsWithSizes),
			Writer:      bytesPerDomainShardedStore,
		},
		transformer.PipelineStage{
			Name:        "FlattenIntoBytesPerDevice",
			Reader:      bytesPerDomainShardedStore,
			Transformer: transformer.TransformFunc(flattenIntoBytesPerDevice),
			Writer:      bytesPerDomainPerDeviceStore,
		},
		transformer.PipelineStage{
			Name:        "FlattenIntoBytesPerTimestamp",
			Reader:      bytesPerDomainShardedStore,
			Transformer: transformer.TransformFunc(flattenIntoBytesPerTimestamp),
			Writer:      bytesPerDomainStore,
		},
		transformer.PipelineStage{
			Name:   "BytesPerDomainPostgresStore",
			Reader: bytesPerDomainStore,
			Writer: bytesPerDomainPostgresStore,
		},
	}, TraceKeyRangesPipeline(newTracesStore, traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

func mapTraceToAddressIdTable(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	if trace.AddressTableFirstId == nil || trace.AddressTableSize == nil {
		panic("AddressTableFirstId and AddressTableSize must be present in all traces")
	}
	baseAddress := *trace.AddressTableFirstId
	maxAddress := *trace.AddressTableSize
	computeAddressIdFromAddressTableOffset := func(offset int) int32 {
		return (baseAddress + int32(offset)) % maxAddress
	}

	for idx, entry := range trace.AddressTableEntry {
		if entry.MacAddress == nil {
			continue
		}
		addressId := computeAddressIdFromAddressTableOffset(idx)
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(traceKey.SessionKey(), addressId, traceKey.SequenceNumber),
			Value: lex.EncodeOrDie(*entry.MacAddress),
		}
	}
}

func lookupPacketTimestampFromId(packetId int32, trace *Trace) int64 {
	if packetId < 0 || packetId >= int32(len(trace.PacketSeries)) {
		panic(fmt.Errorf("packet_id outside packet series: 0 <= %d < %d", packetId, len(trace.PacketSeries)))
	}
	entry := trace.PacketSeries[packetId]
	if entry.TimestampMicroseconds == nil {
		panic(fmt.Errorf("packet series entry missing timestamp"))
	}
	return *entry.TimestampMicroseconds
}

func mapTraceToARecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	for _, entry := range trace.ARecord {
		if entry.AddressId == nil || entry.Domain == nil || entry.Anonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.IpAddress == nil {
			continue
		}
		packetTimestamp := lookupPacketTimestampFromId(*entry.PacketId, trace)
		ttl := convertSecondsToMicroseconds(int64(*entry.Ttl))
		outputChan <- &store.Record{
			Key: lex.EncodeOrDie(traceKey.SessionKey(), *entry.AddressId, traceKey.SequenceNumber, *entry.Domain, *entry.Anonymized, packetTimestamp, packetTimestamp+ttl, *entry.IpAddress),
		}
	}
}

func mapTraceToCnameRecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	for _, entry := range trace.CnameRecord {
		if entry.AddressId == nil || entry.Cname == nil || entry.CnameAnonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.Domain == nil || entry.DomainAnonymized == nil {
			continue
		}
		if *entry.DomainAnonymized {
			continue
		}
		packetTimestamp := lookupPacketTimestampFromId(*entry.PacketId, trace)
		ttl := convertSecondsToMicroseconds(int64(*entry.Ttl))
		outputChan <- &store.Record{
			Key: lex.EncodeOrDie(traceKey.SessionKey(), *entry.AddressId, traceKey.SequenceNumber, *entry.Cname, *entry.CnameAnonymized, packetTimestamp, packetTimestamp+ttl, *entry.Domain),
		}
	}
}

func mapTraceToFlowIpsTable(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	// A flow's "start timestamp" is the timestamp of its first packet.
	// Figure this out using a two step process:
	// 1. Figure out which flow IDs started in the current trace.
	// 2. Find the timestamp of the first packet for each of those flows.
	flowStartTimestamps := make(map[int32]int64)
	missingStartTimestamp := int64(math.MaxInt64)
	for _, entry := range trace.FlowTableEntry {
		if entry.FlowId == nil {
			continue
		}
		flowStartTimestamps[*entry.FlowId] = missingStartTimestamp
	}
	for _, entry := range trace.PacketSeries {
		if entry.FlowId == nil || entry.TimestampMicroseconds == nil {
			continue
		}
		if timestamp, ok := flowStartTimestamps[*entry.FlowId]; ok {
			flowStartTimestamps[*entry.FlowId] = minInt64(timestamp, *entry.TimestampMicroseconds)
		}
	}

	for _, entry := range trace.FlowTableEntry {
		if entry.SourceIp == nil || entry.DestinationIp == nil || entry.FlowId == nil {
			continue
		}
		timestamp, timestampOk := flowStartTimestamps[*entry.FlowId]
		if !timestampOk || timestamp == missingStartTimestamp {
			continue
		}
		outputChan <- &store.Record{
			Key: lex.EncodeOrDie(traceKey.SessionKey(), *entry.SourceIp, traceKey.SequenceNumber, *entry.DestinationIp, timestamp, *entry.FlowId),
		}
		outputChan <- &store.Record{
			Key: lex.EncodeOrDie(traceKey.SessionKey(), *entry.DestinationIp, traceKey.SequenceNumber, *entry.SourceIp, timestamp, *entry.FlowId),
		}
	}
}

func mapTraceToAddressIpTable(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	for _, entry := range trace.AddressTableEntry {
		if entry.MacAddress == nil || entry.IpAddress == nil {
			continue
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(traceKey.SessionKey(), *entry.IpAddress, traceKey.SequenceNumber),
			Value: lex.EncodeOrDie(*entry.MacAddress),
		}
	}
}

func mapTraceToBytesPerTimestampSharded(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	flowIdAndTimestampToSize := make(map[int32]map[int64]int64)
	for _, packetSeriesEntry := range trace.PacketSeries {
		if packetSeriesEntry.FlowId == nil || packetSeriesEntry.TimestampMicroseconds == nil || packetSeriesEntry.Size == nil {
			continue
		}
		if _, ok := flowIdAndTimestampToSize[*packetSeriesEntry.FlowId]; !ok {
			flowIdAndTimestampToSize[*packetSeriesEntry.FlowId] = make(map[int64]int64)
		}
		hourTimestamp := truncateTimestampToHour(*packetSeriesEntry.TimestampMicroseconds)
		flowIdAndTimestampToSize[*packetSeriesEntry.FlowId][hourTimestamp] += int64(*packetSeriesEntry.Size)
	}

	for flowId, timestamps := range flowIdAndTimestampToSize {
		for timestamp, size := range timestamps {
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(traceKey.SessionKey(), flowId, traceKey.SequenceNumber, timestamp),
				Value: lex.EncodeOrDie(size),
			}
		}
	}
}

func mapTraceToWhitelist(traceKey *TraceKey, trace *Trace, outputChan chan *store.Record) {
	if traceKey.SequenceNumber == 0 {
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(traceKey.SessionKey()),
			Value: lex.EncodeOrDie(trace.Whitelist),
		}
	}
}

type traceMapper func(*TraceKey, *Trace, chan *store.Record)

func bytesPerDomainMapper(record *store.Record, outputChans ...chan *store.Record) {
	var traceKey TraceKey
	lex.DecodeOrDie(record.Key, &traceKey)
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}

	defer func() {
		if err := recover(); err != nil {
			log.Printf("Error mapping trace %s,%s,%d,%d: %s", *trace.NodeId, *trace.AnonymizationSignature, *trace.ProcessStartTimeMicroseconds, *trace.TraceCreationTimestamp, err)
		}
	}()

	mappers := []traceMapper{
		mapTraceToAddressIdTable,
		mapTraceToARecordTable,
		mapTraceToCnameRecordTable,
		mapTraceToFlowIpsTable,
		mapTraceToAddressIpTable,
		mapTraceToBytesPerTimestampSharded,
		mapTraceToWhitelist,
	}
	for idx, mapper := range mappers {
		mapper(&traceKey, &trace, outputChans[idx])
	}
}

func joinAddressIdsWithMacAddresses(inputChan, outputChan chan *store.Record) {
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
				macAddress = record.Value
			case 1:
				if macAddress != nil {
					var unusedSequenceNumber int32
					remainder := lex.DecodeOrDie(record.Key, &unusedSequenceNumber)
					outputChan <- &store.Record{
						Key: lex.Concatenate(lex.EncodeOrDie(&session), macAddress, remainder),
					}
				}
			}
		}
	}
}

func emitARecords(record *store.Record, outputChan chan *store.Record) {
	var (
		session                      SessionKey
		macAddress, domain           []byte
		anonymized                   bool
		startTimestamp, endTimestamp int64
		ipAddress                    []byte
	)
	lex.DecodeOrDie(record.Key, &session, &macAddress, &domain, &anonymized, &startTimestamp, &endTimestamp, &ipAddress)

	if anonymized {
		return
	}
	outputChan <- &store.Record{
		Key: lex.EncodeOrDie(&session, domain, macAddress, ipAddress, startTimestamp, endTimestamp),
	}
}

type dnsRecord struct {
	timestamp  int64
	aRecord    bool
	startEvent bool
	value      string
}

type dnsRecords []*dnsRecord

func (records dnsRecords) Len() int { return len(records) }
func (records dnsRecords) Less(i, j int) bool {
	if records[i].timestamp < records[j].timestamp {
		return true
	}
	if records[i].timestamp == records[j].timestamp && records[i].startEvent && !records[j].startEvent {
		return true
	}
	return false
}
func (records dnsRecords) Swap(i, j int) { records[i], records[j] = records[j], records[i] }

func joinARecordsWithCnameRecords(inputChan, outputChan chan *store.Record) {
	var (
		session            SessionKey
		macAddress, domain []byte
		anonymized         bool
	)
	grouper := transformer.GroupRecords(inputChan, &session, &macAddress, &domain, &anonymized)
	for grouper.NextGroup() {
		var allRecords dnsRecords
		for grouper.NextRecord() {
			record := grouper.Read()
			newStartDnsRecord := dnsRecord{startEvent: true, aRecord: record.DatabaseIndex == 0}
			newEndDnsRecord := dnsRecord{startEvent: false, aRecord: record.DatabaseIndex == 0}
			lex.DecodeOrDie(record.Key, &newStartDnsRecord.timestamp, &newEndDnsRecord.timestamp, &newStartDnsRecord.value)
			newEndDnsRecord.value = newStartDnsRecord.value
			allRecords = append(allRecords, &newStartDnsRecord)
			allRecords = append(allRecords, &newEndDnsRecord)
		}
		sort.Sort(allRecords)

		currentAValues := make(map[string]int64)
		currentCnameValues := make(map[string]int64)
		currentACounts := make(map[string]int)
		currentCnameCounts := make(map[string]int)
		for _, record := range allRecords {
			switch record.aRecord {
			case true:
				switch record.startEvent {
				case true:
					timestamp := record.timestamp
					if oldTimestamp, ok := currentAValues[record.value]; ok {
						timestamp = minInt64(timestamp, oldTimestamp)
					}
					currentAValues[record.value] = timestamp
					currentACounts[record.value]++
				case false:
					currentACounts[record.value]--
					if currentACounts[record.value] == 0 {
						startTimestamp := currentAValues[record.value]
						delete(currentAValues, record.value)
						for domain, timestamp := range currentCnameValues {
							outputChan <- &store.Record{
								Key: lex.EncodeOrDie(&session, domain, macAddress, record.value, maxInt64(startTimestamp, timestamp), record.timestamp),
							}
						}
					}
				}
			case false:
				switch record.startEvent {
				case true:
					timestamp := record.timestamp
					if oldTimestamp, ok := currentCnameValues[record.value]; ok {
						timestamp = minInt64(timestamp, oldTimestamp)
					}
					currentCnameValues[record.value] = timestamp
					currentCnameCounts[record.value]++
				case false:
					currentCnameCounts[record.value]--
					if currentCnameCounts[record.value] == 0 {
						startTimestamp := currentCnameValues[record.value]
						delete(currentCnameValues, record.value)
						for ip, timestamp := range currentAValues {
							outputChan <- &store.Record{
								Key: lex.EncodeOrDie(&session, record.value, macAddress, ip, maxInt64(startTimestamp, timestamp), record.timestamp),
							}
						}
					}
				}
			}
		}
	}
}

func joinDomainsWithWhitelist(inputChan, outputChan chan *store.Record) {
	var session SessionKey
	grouper := transformer.GroupRecords(inputChan, &session)
	for grouper.NextGroup() {
		var whitelist []string
		for grouper.NextRecord() {
			record := grouper.Read()

			switch record.DatabaseIndex {
			case 0:
				lex.DecodeOrDie(record.Value, &whitelist)
				sort.Sort(sort.StringSlice(whitelist))
			case 1:
				if whitelist == nil {
					continue
				}
				var domain string
				remainder := lex.DecodeOrDie(record.Key, &domain)
				for i := 0; i < len(domain); i++ {
					if i > 0 && domain[i-1] != '.' {
						continue
					}
					idx := sort.SearchStrings(whitelist, domain[i:])
					if idx >= len(whitelist) || whitelist[idx] != domain[i:] {
						continue
					}
					outputChan <- &store.Record{
						Key: lex.Concatenate(grouper.CurrentGroupPrefix, remainder, lex.EncodeOrDie(whitelist[idx])),
					}
				}
			}
		}
	}
}

func joinMacWithFlowId(inputChan, outputChan chan *store.Record) {
	var (
		session SessionKey
		localIp []byte
	)
	grouper := transformer.GroupRecords(inputChan, &session, &localIp)
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
						remoteIp       []byte
						sequenceNumber int32
						timestamp      int64
						flowId         int32
					)
					lex.DecodeOrDie(record.Key, &sequenceNumber, &remoteIp, &timestamp, &flowId)
					outputChan <- &store.Record{
						Key: lex.EncodeOrDie(&session, macAddress, remoteIp, timestamp, int64(math.MaxInt64), sequenceNumber, flowId),
					}
				}
			}
		}
	}
}

func joinWhitelistedDomainsWithFlows(inputChan, outputChan chan *store.Record) {
	var (
		session              SessionKey
		macAddress, remoteIp []byte
	)
	grouper := transformer.GroupRecords(inputChan, &session, &macAddress, &remoteIp)
	for grouper.NextGroup() {
		type timestampsAndDomain struct {
			start, end int64
			domain     []byte
		}
		var domains []*timestampsAndDomain
		for grouper.NextRecord() {
			record := grouper.Read()

			switch record.DatabaseIndex {
			case 0:
				var newEntry timestampsAndDomain
				lex.DecodeOrDie(record.Key, &newEntry.start, &newEntry.end, &newEntry.domain)
				domains = append(domains, &newEntry)
			case 1:
				if domains != nil {
					var (
						timestamp, unusedInfinity int64
						sequenceNumber, flowId    int32
					)
					lex.DecodeOrDie(record.Key, &timestamp, &unusedInfinity, &sequenceNumber, &flowId)
					for _, entry := range domains {
						if entry.start <= timestamp && entry.end >= timestamp {
							outputChan <- &store.Record{
								Key: lex.EncodeOrDie(&session, flowId, sequenceNumber, entry.domain, macAddress),
							}
						}
					}
				}
			}
		}
	}
}

func groupDomainsAndMacAddresses(inputChan, outputChan chan *store.Record) {
	var (
		session                SessionKey
		flowId, sequenceNumber int32
	)
	grouper := transformer.GroupRecords(inputChan, &session, &flowId, &sequenceNumber)
	for grouper.NextGroup() {
		var domains, macAddresses [][]byte
		for grouper.NextRecord() {
			record := grouper.Read()
			var domain, macAddress []byte
			lex.DecodeOrDie(record.Key, &domain, &macAddress)
			domains = append(domains, domain)
			macAddresses = append(macAddresses, macAddress)
		}
		outputChan <- &store.Record{
			Key:   grouper.CurrentGroupPrefix,
			Value: lex.EncodeOrDie(domains, macAddresses),
		}
	}
}

func joinDomainsWithSizes(inputChan, outputChan chan *store.Record) {
	var (
		session SessionKey
		flowId  int32
	)
	grouper := transformer.GroupRecords(inputChan, &session, &flowId)
	for grouper.NextGroup() {
		var domains, macAddresses [][]byte
		for grouper.NextRecord() {
			record := grouper.Read()

			switch record.DatabaseIndex {
			case 0:
				lex.DecodeOrDie(record.Value, &domains, &macAddresses)
			case 1:
				if domains != nil && macAddresses != nil {
					var (
						sequenceNumber int32
						timestamp      int64
					)
					lex.DecodeOrDie(record.Key, &sequenceNumber, &timestamp)
					for idx, domain := range domains {
						outputChan <- &store.Record{
							Key:   lex.EncodeOrDie(session.NodeId, domain, timestamp, macAddresses[idx], session.AnonymizationContext, session.SessionId, flowId, sequenceNumber),
							Value: record.Value,
						}
					}
				}
			}
		}
	}
}

func flattenIntoBytesPerDevice(inputChan, outputChan chan *store.Record) {
	var (
		nodeId, domain []byte
		timestamp      int64
		macAddress     []byte
	)
	grouper := transformer.GroupRecords(inputChan, &nodeId, &domain, &timestamp, &macAddress)
	for grouper.NextGroup() {
		var totalSize int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var size int64
			lex.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(nodeId, macAddress, domain, timestamp),
			Value: lex.EncodeOrDie(totalSize),
		}
	}
}

func flattenIntoBytesPerTimestamp(inputChan, outputChan chan *store.Record) {
	var (
		nodeId, domain []byte
		timestamp      int64
	)
	grouper := transformer.GroupRecords(inputChan, &nodeId, &domain, &timestamp)
	for grouper.NextGroup() {
		var totalSize int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var size int64
			lex.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(nodeId, domain, timestamp),
			Value: lex.EncodeOrDie(totalSize),
		}
	}
}

type BytesPerDomainPostgresStore struct {
	conn        *sql.DB
	transaction *sql.Tx
	statement   *sql.Stmt
}

func NewBytesPerDomainPostgresStore() *BytesPerDomainPostgresStore {
	return &BytesPerDomainPostgresStore{}
}

func (store *BytesPerDomainPostgresStore) BeginWriting() error {
	conn, err := sql.Open("postgres", "")
	if err != nil {
		return err
	}
	transaction, err := conn.Begin()
	if err != nil {
		conn.Close()
		return err
	}
	if _, err := transaction.Exec("SET search_path TO bismark_passive"); err != nil {
		transaction.Rollback()
		conn.Close()
		return err
	}
	if _, err := transaction.Exec("DELETE FROM bytes_per_domain_per_hour"); err != nil {
		transaction.Rollback()
		conn.Close()
		return err
	}
	statement, err := transaction.Prepare("INSERT INTO bytes_per_domain_per_hour (node_id, domain, timestamp, bytes) VALUES ($1, $2, $3, $4)")
	if err != nil {
		transaction.Rollback()
		conn.Close()
		return err
	}
	store.conn = conn
	store.transaction = transaction
	store.statement = statement
	return nil
}

func (store *BytesPerDomainPostgresStore) WriteRecord(record *store.Record) error {
	var nodeId, domain []byte
	var timestamp, size int64

	lex.DecodeOrDie(record.Key, &nodeId, &domain, &timestamp)
	lex.DecodeOrDie(record.Value, &size)

	if _, err := store.statement.Exec(nodeId, domain, time.Unix(timestamp, 0), size); err != nil {
		return err
	}
	return nil
}

func (store *BytesPerDomainPostgresStore) EndWriting() error {
	if err := store.statement.Close(); err != nil {
		return err
	}
	if err := store.transaction.Commit(); err != nil {
		return err
	}
	if err := store.conn.Close(); err != nil {
		return err
	}
	return nil
}
