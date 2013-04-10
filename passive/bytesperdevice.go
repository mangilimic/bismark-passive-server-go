package passive

import (
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

func BytesPerDevicePipeline(tracesStore, availabilityIntervalsStore transformer.StoreSeeker, sessionsStore, addressTableStore, flowTableStore, packetsStore, flowIdToMacStore, flowIdToMacsStore, bytesPerDeviceUnreducedStore transformer.DatastoreFull, bytesPerDeviceSessionStore, bytesPerDeviceStore transformer.Datastore, bytesPerDevicePostgresStore transformer.StoreWriter, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull, workers int) []transformer.PipelineStage {
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
			Name:        "ReduceBytesPerDeviceSession",
			Reader:      transformer.ReadIncludingPrefixes(bytesPerDeviceUnreducedStore, sessionsStore),
			Transformer: transformer.TransformFunc(ReduceBytesPerDeviceSession),
			Writer:      bytesPerDeviceSessionStore,
		},
		transformer.PipelineStage{
			Name:        "ReduceBytesPerDevice",
			Reader:      bytesPerDeviceSessionStore,
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
	var traceKey TraceKey
	key.DecodeOrDie(record.Key, &traceKey)
	var trace Trace
	if err := proto.Unmarshal(record.Value, &trace); err != nil {
		panic(err)
	}

	MapTraceToAddressTable(&traceKey, &trace, outputChans[0])
	MapTraceToFlowTable(&traceKey, &trace, outputChans[1])
	MapTraceToBytesPerTimestamp(&traceKey, &trace, outputChans[2])
}

func JoinMacAndFlowId(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var session SessionKey
	var ipAddress []byte
	grouper := transformer.GroupRecords(inputChan, &session, &ipAddress)
	for grouper.NextGroup() {
		var currentMacAddress []byte
		for grouper.NextRecord() {
			record := grouper.Read()
			if record.DatabaseIndex == 0 {
				currentMacAddress = record.Value
				continue
			}
			if currentMacAddress == nil {
				continue
			}
			var sequenceNumber int32
			key.DecodeOrDie(record.Key, &sequenceNumber)
			var flowIds []int32
			key.DecodeOrDie(record.Value, &flowIds)
			for _, flowId := range flowIds {
				outputChan <- &transformer.LevelDbRecord{
					Key: key.Join(key.EncodeOrDie(&session, flowId, sequenceNumber), currentMacAddress),
				}
			}
		}
	}
}

func FlattenMacAddresses(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var session SessionKey
	var flowId, sequenceNumber int32
	grouper := transformer.GroupRecords(inputChan, &session, &flowId, &sequenceNumber)
	for grouper.NextGroup() {
		macAddresses := [][]byte{}
		for grouper.NextRecord() {
			record := grouper.Read()
			var macAddress []byte
			key.DecodeOrDie(record.Key, &macAddress)
			macAddresses = append(macAddresses, macAddress)
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(&session, flowId, sequenceNumber),
			Value: key.EncodeOrDie(macAddresses),
		}
	}
}

func JoinMacAndSizes(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var session SessionKey
	var flowId int32
	grouper := transformer.GroupRecords(inputChan, &session, &flowId)
	for grouper.NextGroup() {
		var currentMacAddresses [][]byte
		for grouper.NextRecord() {
			record := grouper.Read()
			if record.DatabaseIndex == 0 {
				key.DecodeOrDie(record.Value, &currentMacAddresses)
				continue
			}
			if currentMacAddresses == nil {
				continue
			}

			var sequenceNumber int32
			key.DecodeOrDie(record.Key, &sequenceNumber)
			var timestamps, sizes []int64
			key.DecodeOrDie(record.Value, &timestamps, &sizes)
			if len(timestamps) != len(sizes) {
				panic(fmt.Errorf("timestamps and sizes must be the same size"))
			}

			for _, currentMacAddress := range currentMacAddresses {
				for idx, timestamp := range timestamps {
					outputChan <- &transformer.LevelDbRecord{
						Key:   key.EncodeOrDie(&session, currentMacAddress, timestamp, flowId, sequenceNumber),
						Value: key.EncodeOrDie(sizes[idx]),
					}
				}
			}
		}
	}
}

func ReduceBytesPerDeviceSession(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var session SessionKey
	var macAddress []byte
	var timestamp int64
	grouper := transformer.GroupRecords(inputChan, &session, &macAddress, &timestamp)
	for grouper.NextGroup() {
		var totalSize int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var size int64
			key.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(session.NodeId, macAddress, timestamp, session.AnonymizationContext, session.SessionId),
			Value: key.EncodeOrDie(totalSize),
		}
	}
}

func ReduceBytesPerDevice(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var nodeId, macAddress []byte
	var timestamp int64
	grouper := transformer.GroupRecords(inputChan, &nodeId, &macAddress, &timestamp)
	for grouper.NextGroup() {
		var totalSize int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var size int64
			key.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(nodeId, macAddress, timestamp),
			Value: key.EncodeOrDie(totalSize),
		}
	}
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
