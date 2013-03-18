package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"log"
	"time"
)

type FlowTimestamp struct {
	flowId    int32
	timestamp int64
}

func BytesPerDevicePipeline(tracesStore, availabilityIntervalsStore transformer.StoreSeeker, addressTableStore, flowTableStore, packetsStore, flowIdToMacStore, flowIdToMacsStore, bytesPerDeviceUnreducedStore transformer.Datastore, bytesPerDeviceStore transformer.StoreWriter, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull, workers int) []transformer.PipelineStage {
	availableTracesStore := transformer.ReadIncludingRanges(tracesStore, availabilityIntervalsStore)
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerDeviceMapper",
			Reader:      transformer.ReadExcludingRanges(availableTracesStore, traceKeyRangesStore),
			Transformer: transformer.MakeMultipleOutputsDoFunc(BytesPerDeviceMapper, 3, workers),
			Writer:      transformer.NewMuxedStoreWriter(addressTableStore, flowTableStore, packetsStore),
		},
		transformer.PipelineStage{
			Name:        "JoinMacAndFlowId",
			Reader:      transformer.NewDemuxStoreReader(addressTableStore, flowTableStore),
			Transformer: transformer.TransformFunc(JoinMacAndFlowId),
			Writer:      flowIdToMacStore,
		},
		transformer.PipelineStage{
			Name:        "FlattenMacAddresses",
			Reader:      flowIdToMacStore,
			Transformer: transformer.TransformFunc(FlattenMacAddresses),
			Writer:      flowIdToMacsStore,
		},
		transformer.PipelineStage{
			Name:        "JoinMacAndSizes",
			Reader:      transformer.NewDemuxStoreReader(flowIdToMacsStore, packetsStore),
			Transformer: transformer.TransformFunc(JoinMacAndSizes),
			Writer:      bytesPerDeviceUnreducedStore,
		},
		transformer.PipelineStage{
			Name:        "ReduceBytesPerDevice",
			Reader:      bytesPerDeviceUnreducedStore,
			Transformer: transformer.TransformFunc(ReduceBytesPerDevice),
			Writer:      bytesPerDeviceStore,
		},
	}, TraceKeyRangesPipeline(transformer.ReadExcludingRanges(availableTracesStore, traceKeyRangesStore), traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

func MapTraceToAddressTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
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

func MapTraceToFlowTable(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	flowIds := make(map[string][]int32)
	for _, entry := range trace.FlowTableEntry {
		if entry.FlowId == nil {
			continue
		}
		if entry.SourceIp != nil {
			flowIds[string(*entry.SourceIp)] = append(flowIds[string(*entry.SourceIp)], *entry.FlowId)
		}
		if entry.DestinationIp != nil {
			flowIds[string(*entry.DestinationIp)] = append(flowIds[string(*entry.DestinationIp)], *entry.FlowId)
		}
	}
	for ipAddress, ids := range flowIds {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, ipAddress, traceKey.SequenceNumber),
			Value: key.EncodeOrDie(ids),
		}
	}
}

func MapTraceToBytesPerTimestamp(traceKey *TraceKey, trace *Trace, outputChan chan *transformer.LevelDbRecord) {
	buckets := make(map[int32]map[int64]int64)
	for _, packetSeriesEntry := range trace.PacketSeries {
		if packetSeriesEntry.FlowId == nil || packetSeriesEntry.TimestampMicroseconds == nil || packetSeriesEntry.Size == nil {
			continue
		}
		timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds*1000)
		minuteTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), 0, 0, time.UTC)
		if _, ok := buckets[*packetSeriesEntry.FlowId]; !ok {
			buckets[*packetSeriesEntry.FlowId] = make(map[int64]int64)
		}
		buckets[*packetSeriesEntry.FlowId][minuteTimestamp.Unix()] += int64(*packetSeriesEntry.Size)
	}
	for flowId, timestampBuckets := range buckets {
		bytesPerTimestamp := BytesPerTimestamp{}
		for timestamp, size := range timestampBuckets {
			entry := BytesPerTimestampEntry{
				Timestamp: proto.Int64(timestamp),
				Size:      proto.Int64(size),
			}
			bytesPerTimestamp.Entry = append(bytesPerTimestamp.Entry, &entry)
		}
		encodedBytesPerTimestamp, err := proto.Marshal(&bytesPerTimestamp)
		if err != nil {
			panic(err)
		}

		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, flowId, traceKey.SequenceNumber),
			Value: encodedBytesPerTimestamp,
		}
		log.Printf("Emitting to bucket: %s,%s,%d,%d,%d", traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, flowId, traceKey.SequenceNumber)
	}
}

func BytesPerDeviceMapper(record *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	traceKey := DecodeTraceKey(record.Key)
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}

	MapTraceToAddressTable(traceKey, &trace, outputChans[0])
	MapTraceToFlowTable(traceKey, &trace, outputChans[1])
	MapTraceToBytesPerTimestamp(traceKey, &trace, outputChans[2])
}

func JoinMacAndFlowId(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSession, currentMacAddress, currentIpAddress []byte
	for record := range inputChan {
		decodedSession, remainder := DecodeSessionKeyWithRemainder(record.Key)
		session := EncodeSessionKey(decodedSession)
		var ipAddress []byte
		var sequenceNumber int32
		key.DecodeOrDie(remainder, &ipAddress, &sequenceNumber)

		if !bytes.Equal(currentSession, session) || !bytes.Equal(currentIpAddress, ipAddress) {
			currentSession = session
			currentIpAddress = nil
			currentMacAddress = nil
		}

		if record.DatabaseIndex == 0 {
			currentIpAddress = ipAddress
			currentMacAddress = record.Value
			continue
		}
		if currentMacAddress == nil {
			continue
		}
		var flowIds []int32
		key.DecodeOrDie(record.Value, &flowIds)
		for _, flowId := range flowIds {
			outputChan <- &transformer.LevelDbRecord{
				Key: key.Join(session, key.EncodeOrDie(flowId, sequenceNumber), currentMacAddress),
			}
		}
	}
	close(outputChan)
}

func FlattenMacAddresses(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentOutputKey []byte
	var macAddresses [][]byte
	for record := range inputChan {
		decodedSession, remainder := DecodeSessionKeyWithRemainder(record.Key)
		session := EncodeSessionKey(decodedSession)
		var flowId, sequenceNumber int32
		var macAddress []byte
		key.DecodeOrDie(remainder, &flowId, &sequenceNumber, &macAddress)

		outputKey := key.Join(session, key.EncodeOrDie(flowId, sequenceNumber))

		if !bytes.Equal(currentOutputKey, outputKey) {
			if currentOutputKey != nil {
				outputChan <- &transformer.LevelDbRecord{
					Key:   currentOutputKey,
					Value: key.EncodeOrDie(macAddresses),
				}
			}
			currentOutputKey = outputKey
			macAddresses = [][]byte{}
		}

		macAddresses = append(macAddresses, macAddress)
	}
	if currentOutputKey != nil {
		outputChan <- &transformer.LevelDbRecord{
			Key:   currentOutputKey,
			Value: key.EncodeOrDie(macAddresses),
		}
	}
	close(outputChan)
}

func JoinMacAndSizes(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSession []byte
	var currentMacAddresses [][]byte
	var currentFlowId int32
	for record := range inputChan {
		session, remainder := DecodeSessionKeyWithRemainder(record.Key)
		encodedSession := EncodeSessionKey(session)
		var flowId, sequenceNumber int32
		key.DecodeOrDie(remainder, &flowId, &sequenceNumber)

		if !bytes.Equal(currentSession, encodedSession) || currentFlowId != flowId {
			currentSession = encodedSession
			currentFlowId = -1
			currentMacAddresses = nil
		}

		if record.DatabaseIndex == 0 {
			currentFlowId = flowId
			key.DecodeOrDie(record.Value, &currentMacAddresses)
			continue
		}
		if currentMacAddresses == nil {
			continue
		}

		bytesPerTimestamp := BytesPerTimestamp{}
		if err := proto.Unmarshal(record.Value, &bytesPerTimestamp); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}

		for _, currentMacAddress := range currentMacAddresses {
			for _, entry := range bytesPerTimestamp.Entry {
				if entry.Timestamp == nil || entry.Size == nil {
					panic(fmt.Errorf("Invalid BytesPerTimestampEntry"))
				}
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(session.NodeId, currentMacAddress, *entry.Timestamp, session.AnonymizationContext, session.SessionId, flowId, sequenceNumber),
					Value: key.EncodeOrDie(*entry.Size),
				}
			}
		}
	}
	close(outputChan)
}

func ReduceBytesPerDevice(inputChan, outputChan chan *transformer.LevelDbRecord) {
	emitReducedBucket := func(nodeId, macAddress []byte, timestamp, size int64) {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(nodeId, macAddress, timestamp),
			Value: key.EncodeOrDie(size),
		}
	}

	var currentNodeId, currentMacAddress []byte
	var currentTimestamp int64
	var currentSize int64
	for record := range inputChan {
		var nodeId, macAddress []byte
		var timestamp int64

		key.DecodeOrDie(record.Key, &nodeId, &macAddress, &timestamp)
		if !bytes.Equal(currentNodeId, nodeId) || !bytes.Equal(currentMacAddress, macAddress) || currentTimestamp != timestamp {
			if currentNodeId != nil && currentMacAddress != nil && currentTimestamp >= 0 {
				emitReducedBucket(currentNodeId, currentMacAddress, currentTimestamp, currentSize)
			}
			currentNodeId = nodeId
			currentMacAddress = macAddress
			currentTimestamp = timestamp
			currentSize = 0
		}

		var size int64
		key.DecodeOrDie(record.Value, &size)
		currentSize += size
		log.Printf("Reducing: %s,%s,%d: %v", nodeId, macAddress, timestamp, size)
	}
	if currentNodeId != nil && currentMacAddress != nil && currentTimestamp >= 0 {
		emitReducedBucket(currentNodeId, currentMacAddress, currentTimestamp, currentSize)
	}
	close(outputChan)
}
