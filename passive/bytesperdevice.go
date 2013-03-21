package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"time"
)

type FlowTimestamp struct {
	flowId    int32
	timestamp int64
}

func BytesPerDevicePipeline(tracesStore, availabilityIntervalsStore transformer.StoreSeeker, sessionsStore, addressTableStore, flowTableStore, packetsStore, flowIdToMacStore, flowIdToMacsStore transformer.DatastoreFull, bytesPerDeviceUnreducedStore transformer.Datastore, bytesPerDeviceStore transformer.Datastore, bytesPerDevicePostgresStore transformer.StoreWriter, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull, workers int) []transformer.PipelineStage {
	newTracesStore := transformer.ReadExcludingRanges(transformer.ReadIncludingRanges(tracesStore, availabilityIntervalsStore), traceKeyRangesStore)
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerDeviceMapper",
			Reader:      newTracesStore,
			Transformer: transformer.MakeMultipleOutputsDoFunc(BytesPerDeviceMapper, 3, workers),
			Writer:      transformer.NewMuxedStoreWriter(addressTableStore, flowTableStore, packetsStore),
		},
		SessionPipelineStage(newTracesStore, sessionsStore),
		transformer.PipelineStage{
			Name:        "JoinMacAndFlowId",
			Reader:      transformer.ReadIncludingPrefixes(transformer.NewDemuxStoreSeeker(addressTableStore, flowTableStore), sessionsStore),
			Transformer: transformer.TransformFunc(JoinMacAndFlowId),
			Writer:      flowIdToMacStore,
		},
		transformer.PipelineStage{
			Name:        "FlattenMacAddresses",
			Reader:      transformer.ReadIncludingPrefixes(flowIdToMacStore, sessionsStore),
			Transformer: transformer.TransformFunc(FlattenMacAddresses),
			Writer:      flowIdToMacsStore,
		},
		transformer.PipelineStage{
			Name:        "JoinMacAndSizes",
			Reader:      transformer.ReadIncludingPrefixes(transformer.NewDemuxStoreSeeker(flowIdToMacsStore, packetsStore), sessionsStore),
			Transformer: transformer.TransformFunc(JoinMacAndSizes),
			Writer:      bytesPerDeviceUnreducedStore,
		},
		transformer.PipelineStage{
			Name:        "ReduceBytesPerDevice",
			Reader:      bytesPerDeviceUnreducedStore,
			Transformer: transformer.TransformFunc(ReduceBytesPerDevice),
			Writer:      bytesPerDeviceStore,
		},
		transformer.PipelineStage{
			Name:   "BytesPerDevicePostgres",
			Reader: bytesPerDeviceStore,
			Writer: bytesPerDevicePostgresStore,
		},
	}, TraceKeyRangesPipeline(newTracesStore, traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
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
		hourTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), 0, 0, 0, time.UTC)
		if _, ok := buckets[*packetSeriesEntry.FlowId]; !ok {
			buckets[*packetSeriesEntry.FlowId] = make(map[int64]int64)
		}
		buckets[*packetSeriesEntry.FlowId][hourTimestamp.Unix()] += int64(*packetSeriesEntry.Size)
	}
	for flowId, timestampBuckets := range buckets {
		timestamps := make([]int64, len(timestampBuckets))
		sizes := make([]int64, len(timestampBuckets))
		idx := 0
		for timestamp, size := range timestampBuckets {
			timestamps[idx] = timestamp
			sizes[idx] = size
			idx++
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.NodeId, traceKey.AnonymizationContext, traceKey.SessionId, flowId, traceKey.SequenceNumber),
			Value: key.EncodeOrDie(timestamps, sizes),
		}
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

		var timestamps, sizes []int64
		key.DecodeOrDie(record.Value, &timestamps, &sizes)
		if len(timestamps) != len(sizes) {
			panic(fmt.Errorf("timestamps and sizes must be the same size"))
		}

		for _, currentMacAddress := range currentMacAddresses {
			for idx, timestamp := range timestamps {
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(session.NodeId, currentMacAddress, timestamp, session.AnonymizationContext, session.SessionId, flowId, sequenceNumber),
					Value: key.EncodeOrDie(sizes[idx]),
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
	}
	if currentNodeId != nil && currentMacAddress != nil && currentTimestamp >= 0 {
		emitReducedBucket(currentNodeId, currentMacAddress, currentTimestamp, currentSize)
	}
	close(outputChan)
}

type BytesPerDevicePostgresStore struct {
	conn        *sql.DB
	transaction *sql.Tx
	statement   *sql.Stmt
}

func NewBytesPerDevicePostgresStore() *BytesPerDevicePostgresStore {
	return &BytesPerDevicePostgresStore{}
}

func (store *BytesPerDevicePostgresStore) BeginWriting() error {
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
	if _, err := transaction.Exec("DELETE FROM bytes_per_device_per_hour"); err != nil {
		transaction.Rollback()
		conn.Close()
		return err
	}
	statement, err := transaction.Prepare("INSERT INTO bytes_per_device_per_hour (node_id, mac_address, timestamp, bytes) VALUES ($1, $2, $3, $4)")
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

func (store *BytesPerDevicePostgresStore) WriteRecord(record *transformer.LevelDbRecord) error {
	var nodeId, macAddress []byte
	var timestamp, size int64

	key.DecodeOrDie(record.Key, &nodeId, &macAddress, &timestamp)
	key.DecodeOrDie(record.Value, &size)

	if _, err := store.statement.Exec(nodeId, macAddress, time.Unix(timestamp, 0), size); err != nil {
		return err
	}
	return nil
}

func (store *BytesPerDevicePostgresStore) EndWriting() error {
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
