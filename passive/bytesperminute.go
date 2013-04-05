package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"database/sql"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"log"
	"time"
)

func BytesPerMinutePipeline(tracesStore transformer.StoreSeeker, mappedStore, bytesPerMinuteStore transformer.Datastore, bytesPerHourStore transformer.Datastore, bytesPerHourPostgresStore transformer.StoreWriter, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull, workers int) []transformer.PipelineStage {
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerMinuteMapper",
			Reader:      transformer.ReadExcludingRanges(tracesStore, traceKeyRangesStore),
			Transformer: transformer.MakeDoTransformer(BytesPerMinuteMapper(transformer.NewNonce()), workers),
			Writer:      mappedStore,
		},
		transformer.PipelineStage{
			Name:        "BytesPerMinuteReducer",
			Reader:      mappedStore,
			Transformer: transformer.TransformFunc(BytesPerMinuteReducer),
			Writer:      bytesPerMinuteStore,
		},
		transformer.PipelineStage{
			Name:        "BytesPerHourReducer",
			Reader:      bytesPerMinuteStore,
			Transformer: transformer.TransformFunc(BytesPerHourReducer),
			Writer:      bytesPerHourStore,
		},
		transformer.PipelineStage{
			Name:   "BytesPerHourPostgres",
			Reader: bytesPerHourStore,
			Writer: bytesPerHourPostgresStore,
		},
	}, TraceKeyRangesPipeline(transformer.ReadExcludingRanges(tracesStore, traceKeyRangesStore), traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

type BytesPerMinuteMapper transformer.Nonce

func (nonce BytesPerMinuteMapper) Do(inputRecord *transformer.LevelDbRecord, outputChan chan *transformer.LevelDbRecord) {
	var traceKey TraceKey
	key.DecodeOrDie(inputRecord.Key, &traceKey)

	trace := Trace{}
	if err := proto.Unmarshal(inputRecord.Value, &trace); err != nil {
		log.Fatalf("Error ummarshaling protocol buffer: %v", err)
	}

	buckets := make(map[int64]int64)
	for _, packetSeriesEntry := range trace.PacketSeries {
		timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds*1000)
		minuteTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), 0, 0, time.UTC)
		buckets[minuteTimestamp.Unix()] += int64(*packetSeriesEntry.Size)
	}

	for timestamp, size := range buckets {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(traceKey.NodeId, timestamp, transformer.Nonce(nonce).Get()),
			Value: key.EncodeOrDie(size),
		}
	}
}

func BytesPerMinuteReducer(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentSize int64
	var currentNode []byte
	currentTimestamp := int64(-1)
	for record := range inputChan {
		var node []byte
		var timestamp int64
		key.DecodeOrDie(record.Key, &node, &timestamp)

		if !bytes.Equal(node, currentNode) || timestamp != currentTimestamp {
			if currentNode != nil && timestamp >= 0 {
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(currentNode, currentTimestamp),
					Value: key.EncodeOrDie(currentSize),
				}
			}
			currentNode = node
			currentTimestamp = timestamp
			currentSize = 0
		}

		var value int64
		key.DecodeOrDie(record.Value, &value)
		currentSize += value
	}
	if currentNode != nil && currentTimestamp >= 0 {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(currentNode, currentTimestamp),
			Value: key.EncodeOrDie(currentSize),
		}
	}
	close(outputChan)
}

func getHour(timestamp int64) int64 {
	t := time.Unix(timestamp, 0)
	hour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	return hour.Unix()
}

func BytesPerHourReducer(inputChan, outputChan chan *transformer.LevelDbRecord) {
	var currentNode []byte
	currentHour := int64(-1)
	var currentSize int64
	for record := range inputChan {
		var node []byte
		var timestamp int64
		key.DecodeOrDie(record.Key, &node, &timestamp)
		hour := getHour(timestamp)

		if !bytes.Equal(node, currentNode) || hour != currentHour {
			if currentNode != nil && currentHour >= 0 {
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(currentNode, currentHour),
					Value: key.EncodeOrDie(currentSize),
				}
			}
			currentNode = node
			currentHour = hour
			currentSize = 0
		}

		var value int64
		key.DecodeOrDie(record.Value, &value)
		currentSize += value
	}
	if currentNode != nil && currentHour >= 0 {
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(currentNode, currentHour),
			Value: key.EncodeOrDie(currentSize),
		}
	}
	close(outputChan)
}

type BytesPerHourPostgresStore struct {
	conn        *sql.DB
	transaction *sql.Tx
	statement   *sql.Stmt
}

func NewBytesPerHourPostgresStore() *BytesPerHourPostgresStore {
	return &BytesPerHourPostgresStore{}
}

func (store *BytesPerHourPostgresStore) BeginWriting() error {
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
	if _, err := transaction.Exec("DELETE FROM bytes_per_hour"); err != nil {
		transaction.Rollback()
		conn.Close()
		return err
	}
	statement, err := transaction.Prepare("INSERT INTO bytes_per_hour (node_id, timestamp, bytes) VALUES ($1, $2, $3)")
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

func (store *BytesPerHourPostgresStore) WriteRecord(record *transformer.LevelDbRecord) error {
	var nodeId []byte
	var timestamp, size int64

	key.DecodeOrDie(record.Key, &nodeId, &timestamp)
	key.DecodeOrDie(record.Value, &size)

	if _, err := store.statement.Exec(nodeId, time.Unix(timestamp, 0), size); err != nil {
		return err
	}
	return nil
}

func (store *BytesPerHourPostgresStore) EndWriting() error {
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
