package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"regexp"
)

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
	return convertMicrosecondsToSeconds(*entry.TimestampMicroseconds)
}

func MapTraceToARecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	for _, entry := range trace.ARecord {
		if entry.AddressId == nil || entry.Domain == nil || entry.Anonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.IpAddress == nil {
			continue
		}
		packetTimestamp := lookupPacketTimestampFromId(*entry.PacketId, trace)
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.SessionKey(), *entry.AddressId, traceKey.SequenceNumber, *entry.Domain, *entry.Anonymized, packetTimestamp, packetTimestamp+int64(*entry.Ttl), *entry.IpAddress),
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
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, *entry.AddressId, traceKey.SequenceNumber, *entry.Cname, *entry.CnameAnonymized, packetTimestamp, packetTimestamp+int64(*entry.Ttl), *entry.Domain),
		}
	}
}

func MapTraceToFlowIpsTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	flowTimestamps := make(map[int32]int64)
	for _, entry := range trace.FlowTableEntry {
		if entry.FlowId == nil {
			continue
		}
		flowTimestamps[*entry.FlowId] = -1
	}
	for _, entry := range trace.PacketSeries {
		if entry.FlowId == nil || entry.TimestampMicroseconds == nil {
			continue
		}
		if timestamp := flowTimestamps[*entry.FlowId]; timestamp < 0 {
			flowTimestamps[*entry.FlowId] = *entry.TimestampMicroseconds
		}
	}
	for _, entry := range trace.FlowTableEntry {
		if entry.SourceIp == nil || entry.DestinationIp == nil || entry.FlowId == nil {
			continue
		}
		timestamp, timestampOk := flowTimestamps[*entry.FlowId]
		if !timestampOk {
			continue
		}
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, *entry.SourceIp, *entry.DestinationIp, traceKey.SequenceNumber, timestamp, *entry.FlowId),
		}
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, *entry.DestinationIp, *entry.SourceIp, traceKey.SequenceNumber, timestamp, *entry.FlowId),
		}
	}
}

func MapTraceToAddressIpTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	for _, entry := range trace.AddressTableEntry {
		if entry.MacAddress == nil || entry.IpAddress == nil {
			continue
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, *entry.IpAddress, traceKey.SequenceNumber),
			Value: key.EncodeOrDie(*entry.MacAddress),
		}
	}
}

func MapTraceToBytesPerTimestampSharded(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	buckets := make(map[int32]map[int64]int64)
	for _, packetSeriesEntry := range trace.PacketSeries {
		if packetSeriesEntry.FlowId == nil || packetSeriesEntry.TimestampMicroseconds == nil || packetSeriesEntry.Size == nil {
			continue
		}
		hourTimestamp := truncateTimestampToHour(*packetSeriesEntry.TimestampMicroseconds)
		if _, ok := buckets[*packetSeriesEntry.FlowId]; !ok {
			buckets[*packetSeriesEntry.FlowId] = make(map[int64]int64)
		}
		buckets[*packetSeriesEntry.FlowId][hourTimestamp] += int64(*packetSeriesEntry.Size)
	}
	for flowId, timestampBuckets := range buckets {
		for timestamp, size := range timestampBuckets {
			outputChan <- &transformer.LevelDbRecord{
				Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, flowId, traceKey.SequenceNumber, timestamp),
				Value: key.EncodeOrDie(size),
			}
		}
	}
}

func MapTraceToWhitelist(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	if traceKey.SequenceNumber > 0 {
		return
	}
	outputChan <- &transformer.LevelDbRecord{
		Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId),
		Value: key.EncodeOrDie(trace.Whitelist),
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
	var currentPrimaryKey []byte
	var currentSession *SessionKey
	var currentMacAddress []byte
	for record := range inputChan {
		var session SessionKey
		var addressId int32
		primaryKey, remainder := key.DecodeAndSplitOrDie(record.Key, &session, &addressId)

		if !bytes.Equal(currentPrimaryKey, primaryKey) {
			currentPrimaryKey = primaryKey
			currentSession = &session
			currentMacAddress = nil
		}

		if record.DatabaseIndex == 0 {
			currentMacAddress = record.Value
			continue
		}
		if currentMacAddress == nil {
			continue
		}
		outputChan <- &transformer.LevelDbRecord{
			Key: key.Join(key.EncodeOrDie(currentSession), currentMacAddress, remainder),
		}
	}
	close(outputChan)
}

func EmitARecords(record *transformer.LevelDbRecord, outputChan chan *transformer.LevelDbRecord) {
	var session SessionKey
	var macAddress, domain []byte
	var anonymized bool
	var startTimestamp, endTimestamp int64
	var ipAddress []byte
	key.DecodeOrDie(record.Key, &session, &macAddress, &domain, &anonymized, &startTimestamp, &endTimestamp, &ipAddress)

	if anonymized {
		return
	}
	outputChan <- &transformer.LevelDbRecord{
		Key: key.EncodeOrDie(session, macAddress, ipAddress, startTimestamp, endTimestamp, domain),
	}
}

type dnsRecord struct {
	startTimestamp, endTimestamp int64
	value                        []byte
}

func JoinARecordsWithCnameRecords(inputChan, outputChan chan *transformer.LevelDbRecord) {
	emitIntersectingRecords := func(keyPrefix []byte, aRecords, cnameRecords []dnsRecord) {
		for _, aRecord := range aRecords {
			for _, cnameRecord := range cnameRecords {
				startTimestamp := maxInt64(aRecord.startTimestamp, cnameRecord.startTimestamp)
				endTimestamp := minInt64(aRecord.endTimestamp, cnameRecord.endTimestamp)
				if startTimestamp >= endTimestamp {
					continue
				}
				outputChan <- &transformer.LevelDbRecord{
					Key: key.Join(keyPrefix, key.EncodeOrDie(aRecord.value, startTimestamp, endTimestamp, cnameRecord.value)),
				}
			}
		}
	}

	var currentPrimaryKey []byte
	var currentSession *SessionKey
	var currentMacAddress []byte
	var aRecords, cnameRecords []dnsRecord
	for record := range inputChan {
		var session SessionKey
		var macAddress, domain []byte
		var anonymized bool
		var startTimestamp, endTimestamp int64
		primaryKey, remainder := key.DecodeAndSplitOrDie(record.Key, &session, &macAddress, &domain, &anonymized)
		domainOrIpRemainder := key.DecodeOrDie(remainder, &startTimestamp, &endTimestamp)
		if !bytes.Equal(currentPrimaryKey, primaryKey) {
			if currentSession != nil && currentMacAddress != nil {
				emitIntersectingRecords(key.EncodeOrDie(currentSession, currentMacAddress), aRecords, cnameRecords)
			}
			currentPrimaryKey = primaryKey
			currentSession = &session
			currentMacAddress = macAddress
			aRecords = nil
			cnameRecords = nil
		}
		switch record.DatabaseIndex {
		case 0:
			var ipAddress []byte
			key.DecodeOrDie(domainOrIpRemainder, &ipAddress)
			aRecords = append(aRecords, dnsRecord{startTimestamp, endTimestamp, ipAddress})
		case 1:
			var domainValue []byte
			key.DecodeOrDie(domainOrIpRemainder, &domainValue)
			cnameRecords = append(cnameRecords, dnsRecord{startTimestamp, endTimestamp, domainValue})
		}
	}
	if currentSession != nil && currentMacAddress != nil {
		emitIntersectingRecords(key.EncodeOrDie(currentSession, currentMacAddress), aRecords, cnameRecords)
	}
	close(outputChan)
}

func JoinDomainsWithWhitelist(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var whitelistRegexps map[string]*regexp.Regexp
	var currentSession *SessionKey
	for record := range inputChan {
		var session SessionKey
		remainder := key.DecodeOrDie(record.Key, &session)
		if !session.Equal(currentSession) {
			currentSession = &session
			whitelistRegexps = nil
		}

		if record.DatabaseIndex == 0 {
			whitelistRegexps = make(map[string]*regexp.Regexp)
			var whitelist []string
			key.DecodeOrDie(record.Value, &whitelist)
			for _, domain := range whitelist {
				compiledRegexp, err := regexp.Compile(fmt.Sprintf(`(^|\.)%s$`, regexp.QuoteMeta(domain)))
				if err != nil {
					panic(fmt.Errorf("Cannot compile whitelist regexp: %s", err))
				}
				whitelistRegexps[domain] = compiledRegexp
			}
			continue
		}

		if whitelistRegexps == nil {
			continue
		}

		var macAddress []byte
		var startTimestamp, endTimestamp int64
		var domain []byte
		key.DecodeOrDie(remainder, &macAddress, &startTimestamp, &endTimestamp, &domain)

		currentKey := key.EncodeOrDie(session, macAddress, startTimestamp, endTimestamp)

		for whitelistDomain, whitelistRegexp := range whitelistRegexps {
			if !whitelistRegexp.Match(domain) {
				continue
			}
			outputChan <- &transformer.LevelDbRecord{
				Key: key.Join(currentKey, key.EncodeOrDie(whitelistDomain)),
			}
		}
	}
	close(outputChan)
}

func JoinMacWithFlowId(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSession *SessionKey
	var currentPrimaryKey, currentLocalIp, currentMacAddress []byte
	for record := range inputChan {
		var session SessionKey
		var localIp []byte
		primaryKey, remainder := key.DecodeAndSplitOrDie(record.Key, &session, &localIp)
		if !bytes.Equal(currentPrimaryKey, primaryKey) {
			currentPrimaryKey = primaryKey
			currentSession = &session
			currentLocalIp = nil
			currentMacAddress = nil
		}
		if record.DatabaseIndex == 0 {
			currentLocalIp = localIp
			key.DecodeOrDie(record.Value, &currentMacAddress)
			continue
		}

		if currentLocalIp == nil || currentMacAddress == nil {
			continue
		}

		var remoteIp []byte
		var sequenceNumber int32
		var timestamp int64
		var flowId int32
		key.DecodeOrDie(remainder, &remoteIp, &sequenceNumber, &timestamp, &flowId)
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(currentSession, currentMacAddress, remoteIp, timestamp, sequenceNumber, flowId),
		}
	}
	close(outputChan)
}

type timestampTuple struct {
	start, end int64
	domain     []byte
}

func JoinWhitelistedDomainsWithFlows(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSession *SessionKey
	var currentPrimaryKey, currentMacAddress []byte
	var currentDomains []timestampTuple
	for record := range inputChan {
		var session SessionKey
		var macAddress, remoteIp []byte
		primaryKey, remainder := key.DecodeAndSplitOrDie(record.Key, &session, &macAddress, &remoteIp)

		if !bytes.Equal(currentPrimaryKey, primaryKey) {
			currentPrimaryKey = primaryKey
			currentSession = &session
			currentMacAddress = macAddress
			currentDomains = nil
		}

		if record.DatabaseIndex == 0 {
			var startTimestamp, endTimestamp int64
			var domain []byte
			key.DecodeOrDie(remainder, &startTimestamp, &endTimestamp, &domain)
			currentDomains = append(currentDomains, timestampTuple{startTimestamp, endTimestamp, domain})
			continue
		}

		var timestamp int64
		var sequenceNumber int32
		var flowId int32
		key.DecodeOrDie(remainder, &timestamp, &sequenceNumber, &flowId)

		for _, entry := range currentDomains {
			if entry.start <= timestamp && entry.end >= timestamp {
				outputChan <- &transformer.LevelDbRecord{
					Key: key.EncodeOrDie(currentSession, flowId, sequenceNumber, entry.domain, currentMacAddress),
				}
			}
		}
	}
}

func JoinDomainsWithSizes(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentPrimaryKey []byte
	var currentSession *SessionKey
	var currentSequenceNumber int32
	var currentDomains map[string][]string
	for record := range inputChan {
		var session SessionKey
		var sequenceNumber, flowId int32
		primaryKey, remainder := key.DecodeAndSplitOrDie(record.Key, &session, &flowId)
		key.DecodeOrDie(remainder, &sequenceNumber)

		if !bytes.Equal(primaryKey, currentPrimaryKey) {
			currentPrimaryKey = primaryKey
			currentSession = &session
			currentDomains = make(map[string][]string)
		}

		switch record.DatabaseIndex {
		case 0:
			if currentSequenceNumber != sequenceNumber {
				currentSequenceNumber = sequenceNumber
				currentDomains = make(map[string][]string)
			}

			var domain, macAddress string
			key.DecodeOrDie(remainder, &domain, &macAddress)
			currentDomains[macAddress] = append(currentDomains[macAddress], domain)
		case 1:
			var timestamp int64
			key.DecodeOrDie(remainder, &timestamp)

			for macAddress, domains := range currentDomains {
				for _, domain := range domains {
					outputChan <- &transformer.LevelDbRecord{
						Key:   key.EncodeOrDie(currentSession, domain, timestamp, macAddress, flowId, sequenceNumber),
						Value: record.Value,
					}
				}
			}
		}
	}
}

func FlattenIntoBytesPerDevice(inputChan, outputChan chan *transformer.LevelDbRecord) {
	emitSize := func(session *SessionKey, macAddress, domain []byte, timestamp, size int64) {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(session, macAddress, domain, timestamp),
			Value: key.EncodeOrDie(size),
		}
	}

	var currentPrimaryKey []byte
	var currentSession *SessionKey
	var currentDomain []byte
	var currentTimestamp int64
	var currentMacAddress []byte
	var currentSize int64
	for record := range inputChan {
		var session SessionKey
		var domain []byte
		var timestamp int64
		var macAddress []byte
		primaryKey, _ := key.DecodeAndSplitOrDie(record.Key, &session, &domain, &timestamp, &macAddress)
		if !bytes.Equal(currentPrimaryKey, primaryKey) {
			if currentPrimaryKey != nil {
				emitSize(currentSession, currentMacAddress, currentDomain, currentTimestamp, currentSize)
			}
			currentPrimaryKey = primaryKey
			currentSession = &session
			currentMacAddress = macAddress
			currentDomain = domain
			currentTimestamp = timestamp
			currentSize = 0
		}
		var size int64
		key.DecodeOrDie(record.Value, &size)
		currentSize += size
	}
	if currentPrimaryKey != nil {
		emitSize(currentSession, currentMacAddress, currentDomain, currentTimestamp, currentSize)
	}
}

func FlattenIntoBytesPerTimestamp(inputChan, outputChan chan *transformer.LevelDbRecord) {
	emitSize := func(primaryKey []byte, size int64) {
		outputChan <- &transformer.LevelDbRecord{
			Key:   primaryKey,
			Value: key.EncodeOrDie(size),
		}
	}

	var currentPrimaryKey []byte
	var currentSize int64
	for record := range inputChan {
		var session SessionKey
		var domain []byte
		var timestamp int64
		primaryKey, _ := key.DecodeAndSplitOrDie(record.Key, &session, &domain, &timestamp)
		if !bytes.Equal(currentPrimaryKey, primaryKey) {
			if currentPrimaryKey != nil {
				emitSize(currentPrimaryKey, currentSize)
			}
			currentPrimaryKey = primaryKey
			currentSize = 0
		}
		var size int64
		key.DecodeOrDie(record.Value, &size)
		currentSize += size
	}
	if currentPrimaryKey != nil {
		emitSize(currentPrimaryKey, currentSize)
	}
}
