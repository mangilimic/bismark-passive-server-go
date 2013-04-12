package passive

import (
	"code.google.com/p/goprotobuf/proto"
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"math"
	"sort"
	"time"
)

type BytesPerDomainPipelineStores struct {
	Traces                                     transformer.StoreSeeker
	AvailabilityIntervals                      transformer.StoreSeeker
	TraceKeyRanges, ConsolidatedTraceKeyRanges transformer.DatastoreFull

	// Outputs of the first stage.
	AddressIdTable           transformer.DatastoreFull
	ARecordTable             transformer.DatastoreFull
	CnameRecordTable         transformer.DatastoreFull
	FlowIpsTable             transformer.DatastoreFull
	AddressIpTable           transformer.DatastoreFull
	BytesPerTimestampSharded transformer.DatastoreFull
	Whitelist                transformer.DatastoreFull

	ARecordsWithMac, CnameRecordsWithMac, AllDnsMappings, AllWhitelistedMappings transformer.DatastoreFull
	FlowMacsTable, FlowDomainsTable, FlowDomainsGroupedTable                     transformer.DatastoreFull

	BytesPerDomainSharded                   transformer.Datastore
	BytesPerDomainPerDevice, BytesPerDomain transformer.Datastore

	BytesPerDomainPostgres transformer.StoreWriter

	Sessions transformer.DatastoreFull
}

func BytesPerDomainPipeline(stores *BytesPerDomainPipelineStores, workers int) []transformer.PipelineStage {
	excludeOldSessions := func(store transformer.StoreSeeker) transformer.StoreSeeker {
		return transformer.ReadIncludingPrefixes(store, stores.Sessions)
	}
	newTracesStore := transformer.ReadExcludingRanges(transformer.ReadIncludingRanges(stores.Traces, stores.AvailabilityIntervals), stores.TraceKeyRanges)
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerDomainMapper",
			Reader:      newTracesStore,
			Transformer: transformer.MakeMultipleOutputsDoFunc(BytesPerDomainMapper, 7, workers),
			Writer:      transformer.NewMuxedStoreWriter(stores.AddressIdTable, stores.ARecordTable, stores.CnameRecordTable, stores.FlowIpsTable, stores.AddressIpTable, stores.BytesPerTimestampSharded, stores.Whitelist),
		},
		SessionPipelineStage(newTracesStore, stores.Sessions),
		transformer.PipelineStage{
			Name:        "JoinAAddressIdsWithMacAddresses",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.AddressIdTable, stores.ARecordTable)),
			Transformer: transformer.TransformFunc(JoinAddressIdsWithMacAddresses),
			Writer:      stores.ARecordsWithMac,
		},
		transformer.PipelineStage{
			Name:        "JoinCnameAddressIdsWithMacAddresses",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.AddressIdTable, stores.CnameRecordTable)),
			Transformer: transformer.TransformFunc(JoinAddressIdsWithMacAddresses),
			Writer:      stores.CnameRecordsWithMac,
		},
		transformer.PipelineStage{
			Name:        "JoinARecordsWithCnameRecords",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.ARecordsWithMac, stores.CnameRecordsWithMac)),
			Transformer: transformer.TransformFunc(JoinARecordsWithCnameRecords),
			Writer:      stores.AllDnsMappings,
		},
		transformer.PipelineStage{
			Name:        "EmitARecords",
			Reader:      excludeOldSessions(stores.ARecordsWithMac),
			Transformer: transformer.MakeDoFunc(EmitARecords, workers),
			Writer:      stores.AllDnsMappings,
		},
		transformer.PipelineStage{
			Name:        "JoinDomainsWithWhitelist",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.Whitelist, stores.AllDnsMappings)),
			Transformer: transformer.TransformFunc(JoinDomainsWithWhitelist),
			Writer:      stores.AllWhitelistedMappings,
		},
		transformer.PipelineStage{
			Name:        "JoinMacWithFlowId",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.AddressIpTable, stores.FlowIpsTable)),
			Transformer: transformer.TransformFunc(JoinMacWithFlowId),
			Writer:      stores.FlowMacsTable,
		},
		transformer.PipelineStage{
			Name:        "JoinWhitelistedDomainsWithFlows",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.AllWhitelistedMappings, stores.FlowMacsTable)),
			Transformer: transformer.TransformFunc(JoinWhitelistedDomainsWithFlows),
			Writer:      stores.FlowDomainsTable,
		},
		transformer.PipelineStage{
			Name:        "GroupDomainsAndMacAddresses",
			Reader:      excludeOldSessions(stores.FlowDomainsTable),
			Transformer: transformer.TransformFunc(GroupDomainsAndMacAddresses),
			Writer:      stores.FlowDomainsGroupedTable,
		},
		transformer.PipelineStage{
			Name:        "JoinDomainsWithSizes",
			Reader:      excludeOldSessions(transformer.NewDemuxStoreSeeker(stores.FlowDomainsGroupedTable, stores.BytesPerTimestampSharded)),
			Transformer: transformer.TransformFunc(JoinDomainsWithSizes),
			Writer:      stores.BytesPerDomainSharded,
		},
		transformer.PipelineStage{
			Name:        "FlattenIntoBytesPerDevice",
			Reader:      stores.BytesPerDomainSharded,
			Transformer: transformer.TransformFunc(FlattenIntoBytesPerDevice),
			Writer:      stores.BytesPerDomainPerDevice,
		},
		transformer.PipelineStage{
			Name:        "FlattenIntoBytesPerTimestamp",
			Reader:      stores.BytesPerDomainSharded,
			Transformer: transformer.TransformFunc(FlattenIntoBytesPerTimestamp),
			Writer:      stores.BytesPerDomain,
		},
		transformer.PipelineStage{
			Name:   "BytesPerDomainPostgresStore",
			Reader: stores.BytesPerDomain,
			Writer: stores.BytesPerDomainPostgres,
		},
	}, TraceKeyRangesPipeline(newTracesStore, stores.TraceKeyRanges, stores.ConsolidatedTraceKeyRanges)...)
}

func MapTraceToAddressIdTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
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
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.SessionKey(), addressId, traceKey.SequenceNumber),
			Value: key.EncodeOrDie(*entry.MacAddress),
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

func MapTraceToARecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	for _, entry := range trace.ARecord {
		if entry.AddressId == nil || entry.Domain == nil || entry.Anonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.IpAddress == nil {
			continue
		}
		packetTimestamp := lookupPacketTimestampFromId(*entry.PacketId, trace)
		ttl := convertSecondsToMicroseconds(int64(*entry.Ttl))
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.SessionKey(), *entry.AddressId, traceKey.SequenceNumber, *entry.Domain, *entry.Anonymized, packetTimestamp, packetTimestamp+ttl, *entry.IpAddress),
		}
	}
}

func MapTraceToCnameRecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	for _, entry := range trace.CnameRecord {
		if entry.AddressId == nil || entry.Cname == nil || entry.CnameAnonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.Domain == nil || entry.DomainAnonymized == nil {
			continue
		}
		if *entry.DomainAnonymized {
			continue
		}
		packetTimestamp := lookupPacketTimestampFromId(*entry.PacketId, trace)
		ttl := convertSecondsToMicroseconds(int64(*entry.Ttl))
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.SessionKey(), *entry.AddressId, traceKey.SequenceNumber, *entry.Cname, *entry.CnameAnonymized, packetTimestamp, packetTimestamp+ttl, *entry.Domain),
		}
	}
}

func MapTraceToFlowIpsTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
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
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.SessionKey(), *entry.SourceIp, traceKey.SequenceNumber, *entry.DestinationIp, timestamp, *entry.FlowId),
		}
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.SessionKey(), *entry.DestinationIp, traceKey.SequenceNumber, *entry.SourceIp, timestamp, *entry.FlowId),
		}
	}
}

func MapTraceToAddressIpTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	for _, entry := range trace.AddressTableEntry {
		if entry.MacAddress == nil || entry.IpAddress == nil {
			continue
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.SessionKey(), *entry.IpAddress, traceKey.SequenceNumber),
			Value: key.EncodeOrDie(*entry.MacAddress),
		}
	}
}

func MapTraceToBytesPerTimestampSharded(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
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
			outputChan <- &transformer.LevelDbRecord{
				Key:   key.EncodeOrDie(traceKey.SessionKey(), flowId, traceKey.SequenceNumber, timestamp),
				Value: key.EncodeOrDie(size),
			}
		}
	}
}

func MapTraceToWhitelist(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	if traceKey.SequenceNumber == 0 {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.SessionKey()),
			Value: key.EncodeOrDie(trace.Whitelist),
		}
	}
}

type traceMapper func(*TraceKey, *Trace, chan *transformer.LevelDbRecord)

func BytesPerDomainMapper(record *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	var traceKey TraceKey
	key.DecodeOrDie(record.Key, &traceKey)
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}

	mappers := []traceMapper{
		MapTraceToAddressIdTable,
		MapTraceToARecordTable,
		MapTraceToCnameRecordTable,
		MapTraceToFlowIpsTable,
		MapTraceToAddressIpTable,
		MapTraceToBytesPerTimestampSharded,
		MapTraceToWhitelist,
	}
	for idx, mapper := range mappers {
		mapper(&traceKey, &trace, outputChans[idx])
	}
}

func JoinAddressIdsWithMacAddresses(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
					remainder := key.DecodeOrDie(record.Key, &unusedSequenceNumber)
					outputChan <- &transformer.LevelDbRecord{
						Key: key.Join(key.EncodeOrDie(&session), macAddress, remainder),
					}
				}
			}
		}
	}
}

func EmitARecords(record *transformer.LevelDbRecord, outputChan chan *transformer.LevelDbRecord) {
	var (
		session                      SessionKey
		macAddress, domain           []byte
		anonymized                   bool
		startTimestamp, endTimestamp int64
		ipAddress                    []byte
	)
	key.DecodeOrDie(record.Key, &session, &macAddress, &domain, &anonymized, &startTimestamp, &endTimestamp, &ipAddress)

	if anonymized {
		return
	}
	outputChan <- &transformer.LevelDbRecord{
		Key: key.EncodeOrDie(&session, domain, macAddress, ipAddress, startTimestamp, endTimestamp),
	}
}

func JoinARecordsWithCnameRecords(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var (
		session            SessionKey
		macAddress, domain []byte
		anonymized         bool
	)
	grouper := transformer.GroupRecords(inputChan, &session, &macAddress, &domain, &anonymized)
	for grouper.NextGroup() {
		type dnsRecord struct {
			startTimestamp, endTimestamp int64
			value                        []byte
		}
		var aRecords, cnameRecords []dnsRecord
		for grouper.NextRecord() {
			record := grouper.Read()
			var newDnsRecord dnsRecord
			key.DecodeOrDie(record.Key, &newDnsRecord.startTimestamp, &newDnsRecord.endTimestamp, &newDnsRecord.value)
			switch record.DatabaseIndex {
			case 0:
				aRecords = append(aRecords, newDnsRecord)
			case 1:
				cnameRecords = append(cnameRecords, newDnsRecord)
			default:
				panic(fmt.Errorf("Invalid DatabaseIndex: %d", record.DatabaseIndex))
			}
		}
		for _, aRecord := range aRecords {
			for _, cnameRecord := range cnameRecords {
				startTimestamp := maxInt64(aRecord.startTimestamp, cnameRecord.startTimestamp)
				endTimestamp := minInt64(aRecord.endTimestamp, cnameRecord.endTimestamp)
				if startTimestamp >= endTimestamp {
					continue
				}
				outputChan <- &transformer.LevelDbRecord{
					Key: key.EncodeOrDie(&session, cnameRecord.value, macAddress, aRecord.value, startTimestamp, endTimestamp),
				}
			}
		}
	}
}

func JoinDomainsWithWhitelist(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var session SessionKey
	grouper := transformer.GroupRecords(inputChan, &session)
	for grouper.NextGroup() {
		var whitelist []string
		for grouper.NextRecord() {
			record := grouper.Read()

			switch record.DatabaseIndex {
			case 0:
				key.DecodeOrDie(record.Value, &whitelist)
				sort.Sort(sort.StringSlice(whitelist))
			case 1:
				if whitelist == nil {
					continue
				}
				var domain string
				remainder := key.DecodeOrDie(record.Key, &domain)
				for i := 0; i < len(domain); i++ {
					if i > 0 && domain[i-1] != '.' {
						continue
					}
					idx := sort.SearchStrings(whitelist, domain[i:])
					if idx >= len(whitelist) || whitelist[idx] != domain[i:] {
						continue
					}
					outputChan <- &transformer.LevelDbRecord{
						Key: key.Join(grouper.CurrentGroupPrefix, remainder, key.EncodeOrDie(whitelist[idx])),
					}
				}
			}
		}
	}
}

func JoinMacWithFlowId(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
				key.DecodeOrDie(record.Value, &macAddress)
			case 1:
				if macAddress != nil {
					var (
						remoteIp       []byte
						sequenceNumber int32
						timestamp      int64
						flowId         int32
					)
					key.DecodeOrDie(record.Key, &sequenceNumber, &remoteIp, &timestamp, &flowId)
					outputChan <- &transformer.LevelDbRecord{
						Key: key.EncodeOrDie(&session, macAddress, remoteIp, timestamp, int64(math.MaxInt64), sequenceNumber, flowId),
					}
				}
			}
		}
	}
}

func JoinWhitelistedDomainsWithFlows(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
				key.DecodeOrDie(record.Key, &newEntry.start, &newEntry.end, &newEntry.domain)
				domains = append(domains, &newEntry)
			case 1:
				if domains != nil {
					var (
						timestamp, unusedInfinity int64
						sequenceNumber, flowId    int32
					)
					key.DecodeOrDie(record.Key, &timestamp, &unusedInfinity, &sequenceNumber, &flowId)
					for _, entry := range domains {
						if entry.start <= timestamp && entry.end >= timestamp {
							outputChan <- &transformer.LevelDbRecord{
								Key: key.EncodeOrDie(&session, flowId, sequenceNumber, entry.domain, macAddress),
							}
						}
					}
				}
			}
		}
	}
}

func GroupDomainsAndMacAddresses(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
			key.DecodeOrDie(record.Key, &domain, &macAddress)
			domains = append(domains, domain)
			macAddresses = append(macAddresses, macAddress)
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   grouper.CurrentGroupPrefix,
			Value: key.EncodeOrDie(domains, macAddresses),
		}
	}
}

func JoinDomainsWithSizes(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
				key.DecodeOrDie(record.Value, &domains, &macAddresses)
			case 1:
				if domains != nil && macAddresses != nil {
					var (
						sequenceNumber int32
						timestamp      int64
					)
					key.DecodeOrDie(record.Key, &sequenceNumber, &timestamp)
					for idx, domain := range domains {
						outputChan <- &transformer.LevelDbRecord{
							Key:   key.EncodeOrDie(session.NodeId, domain, timestamp, macAddresses[idx], session.AnonymizationContext, session.SessionId, flowId, sequenceNumber),
							Value: record.Value,
						}
					}
				}
			}
		}
	}
}

func FlattenIntoBytesPerDevice(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
			key.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(nodeId, macAddress, domain, timestamp),
			Value: key.EncodeOrDie(totalSize),
		}
	}
}

func FlattenIntoBytesPerTimestamp(inputChan, outputChan chan *transformer.LevelDbRecord) {
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
			key.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(nodeId, domain, timestamp),
			Value: key.EncodeOrDie(totalSize),
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

func (store *BytesPerDomainPostgresStore) WriteRecord(record *transformer.LevelDbRecord) error {
	var nodeId, domain []byte
	var timestamp, size int64

	key.DecodeOrDie(record.Key, &nodeId, &domain, &timestamp)
	key.DecodeOrDie(record.Value, &size)

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
