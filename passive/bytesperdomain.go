package passive

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"time"
)

func makeAddressIdComputer(trace *Trace) func(int) int32 {
	if trace.AddressTableFirstId == nil || trace.AddressTableSize == nil {
		panic("AddressTableFirstId and AddressTableSize must be present in all traces")
	}
	baseAddress := *trace.AddressTableFirstId
	maxAddress := *trace.AddressTableSize
	return func(offset int) int32 {
		return (baseAddress + int32(offset)) % maxAddress
	}
}

func MapTraceToAddressIdTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	computeAddressId := makeAddressIdComputer(trace)
	for idx, entry := range trace.AddressTableEntry {
		if entry.MacAddress == nil {
			continue
		}
		addressId := computeAddressId(idx)
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, addressId, traceKey.SequenceNumber),
			Value: key.EncodeOrDie(*entry.MacAddress),
		}
	}
}

func getPacketTimestamp(packetId int32, trace *Trace) int64 {
	if packetId < 0 || packetId >= int32(len(trace.PacketSeries)) {
		panic(fmt.Errorf("packet_id outside packet series: 0 <= %d < %d", packetId, len(trace.PacketSeries)))
	}
	entry := trace.PacketSeries[packetId]
	if entry == nil || entry.TimestampMicroseconds == nil {
		panic(fmt.Errorf("packet series entry is either nil or missing timestamp"))
	}
	return *entry.TimestampMicroseconds / int64(1000000)
}

func MapTraceToARecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	computeAddressId := makeAddressIdComputer(trace)
	for idx, entry := range trace.ARecord {
		if entry.Domain == nil || entry.Anonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.IpAddress == nil {
			continue
		}
		addressId := computeAddressId(idx)
		packetTimestamp := getPacketTimestamp(*entry.PacketId, trace)
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, addressId, traceKey.SequenceNumber, *entry.Domain, *entry.Anonymized, packetTimestamp, packetTimestamp+int64(*entry.Ttl), *entry.IpAddress),
		}
	}
}

func MapTraceToCnameRecordTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	computeAddressId := makeAddressIdComputer(trace)
	for idx, entry := range trace.CnameRecord {
		if entry.Cname == nil || entry.CnameAnonymized == nil || entry.PacketId == nil || entry.Ttl == nil || entry.Domain == nil || entry.DomainAnonymized == nil {
			continue
		}
		if *entry.DomainAnonymized {
			continue
		}
		addressId := computeAddressId(idx)
		packetTimestamp := getPacketTimestamp(*entry.PacketId, trace)
		outputChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, addressId, traceKey.SequenceNumber, *entry.Cname, *entry.CnameAnonymized, packetTimestamp, packetTimestamp+int64(*entry.Ttl), *entry.Domain),
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
		timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds*1000)
		hourTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), 0, 0, 0, time.UTC)
		if _, ok := buckets[*packetSeriesEntry.FlowId]; !ok {
			buckets[*packetSeriesEntry.FlowId] = make(map[int64]int64)
		}
		buckets[*packetSeriesEntry.FlowId][hourTimestamp.Unix()] += int64(*packetSeriesEntry.Size)
	}
	for flowId, timestampBuckets := range buckets {
		for timestamp, size := range timestampBuckets {
			outputChan <- &transformer.LevelDbRecord{
				Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, flowId, timestamp),
				Value: key.EncodeOrDie(size),
			}
		}
	}
}

func BytesPerDomainMapper(record *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	traceKey := DecodeTraceKey(record.Key)
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}

	MapTraceToAddressIdTable(traceKey, &trace, outputChans[0])
	MapTraceToARecordTable(traceKey, &trace, outputChans[1])
	MapTraceToCnameRecordTable(traceKey, &trace, outputChans[2])
	MapTraceToFlowIpsTable(traceKey, &trace, outputChans[3])
	MapTraceToAddressIpTable(traceKey, &trace, outputChans[4])
	MapTraceToBytesPerTimestampSharded(traceKey, &trace, outputChans[5])
}

func JoinAddressIdsWithMacAddresses(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSession, currentMacAddress []byte
    var currentAddressId int32
	for record := range inputChan {
		session, remainderWithAddressId := DecodeSessionKeyWithRemainder(record.Key)
		session := EncodeSessionKey(decodedSession)
		var addressId int32
		remainder := key.DecodeOrDie(remainderWithAddressId, &addressId)

		if currentSession == nil || !currentSession.Equal(session) || currentAddressId != addressId {
			currentSession = session
            currentAddressId = addressId
			currentMacAddress = nil
		}

		if record.DatabaseIndex == 0 {
			currentMacAddress = record.Value
			continue
		}
		if currentMacAddress == nil {
			continue
		}
		for _, flowId := range flowIds {
			outputChan <- &transformer.LevelDbRecord{
				Key: key.Join(session, currentMacAddress, remainder),
			}
		}
	}
	close(outputChan)
}
