package passive

import (
	"bytes"
	"database/sql"
	"log"
	"time"

	"code.google.com/p/goprotobuf/proto"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func BytesPerMinutePipeline(levelDbManager store.Manager, bytesPerHourPostgresStore store.Writer, workers int) transformer.Pipeline {
	tracesStore := levelDbManager.Seeker("traces")
	mappedStore := levelDbManager.ReadingWriter("bytesperminute-mapped")
	bytesPerMinuteStore := levelDbManager.ReadingWriter("bytesperminute")
	bytesPerHourStore := levelDbManager.ReadingWriter("bytesperhour")
	traceKeyRangesStore := levelDbManager.ReadingDeleter("bytesperminute-trace-key-ranges")
	consolidatedTraceKeyRangesStore := levelDbManager.ReadingDeleter("bytesperminute-consolidated-trace-key-ranges")
	return append([]transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerMinuteMapper",
			Reader:      store.NewRangeExcludingReader(tracesStore, traceKeyRangesStore),
			Transformer: transformer.MakeDoTransformer(bytesPerMinuteMapper(transformer.NewNonce()), workers),
			Writer:      mappedStore,
		},
		transformer.PipelineStage{
			Name:        "BytesPerMinuteReducer",
			Reader:      mappedStore,
			Transformer: transformer.TransformFunc(bytesPerMinuteReducer),
			Writer:      bytesPerMinuteStore,
		},
		transformer.PipelineStage{
			Name:        "BytesPerHourReducer",
			Reader:      bytesPerMinuteStore,
			Transformer: transformer.TransformFunc(bytesPerHourReducer),
			Writer:      bytesPerHourStore,
		},
		transformer.PipelineStage{
			Name:   "BytesPerHourPostgres",
			Reader: bytesPerHourStore,
			Writer: bytesPerHourPostgresStore,
		},
	}, TraceKeyRangesPipeline(store.NewRangeExcludingReader(tracesStore, traceKeyRangesStore), traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

type bytesPerMinuteMapper transformer.Nonce

func (nonce bytesPerMinuteMapper) Do(inputRecord *store.Record, outputChan chan *store.Record) {
	var traceKey TraceKey
	lex.DecodeOrDie(inputRecord.Key, &traceKey)

	trace := Trace{}
	if err := proto.Unmarshal(inputRecord.Value, &trace); err != nil {
		log.Fatalf("Error ummarshaling protocol buffer: %v", err)
	}

	buckets := make(map[int64]int64)
	for _, packetSeriesEntry := range trace.PacketSeries {
		timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds*1000)
		minuteTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), 0, 0, timestamp.Location())
		buckets[minuteTimestamp.Unix()] += int64(*packetSeriesEntry.Size)
	}

	for timestamp, size := range buckets {
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(traceKey.NodeId, timestamp, transformer.Nonce(nonce).Get()),
			Value: lex.EncodeOrDie(size),
		}
	}
}

func bytesPerMinuteReducer(inputChan, outputChan chan *store.Record) {
	var node []byte
	var timestamp int64
	grouper := transformer.GroupRecords(inputChan, &node, &timestamp)
	for grouper.NextGroup() {
		var totalSize int64
		for grouper.NextRecord() {
			record := grouper.Read()
			var size int64
			lex.DecodeOrDie(record.Value, &size)
			totalSize += size
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(node, timestamp),
			Value: lex.EncodeOrDie(totalSize),
		}
	}
}

func getHour(timestamp int64) int64 {
	t := time.Unix(timestamp, 0)
	hour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	return hour.Unix()
}

func bytesPerHourReducer(inputChan, outputChan chan *store.Record) {
	var currentNode []byte
	currentHour := int64(-1)
	var currentSize int64
	for record := range inputChan {
		var node []byte
		var timestamp int64
		lex.DecodeOrDie(record.Key, &node, &timestamp)
		hour := getHour(timestamp)

		if !bytes.Equal(node, currentNode) || hour != currentHour {
			if currentNode != nil && currentHour >= 0 {
				outputChan <- &store.Record{
					Key:   lex.EncodeOrDie(currentNode, currentHour),
					Value: lex.EncodeOrDie(currentSize),
				}
			}
			currentNode = node
			currentHour = hour
			currentSize = 0
		}

		var value int64
		lex.DecodeOrDie(record.Value, &value)
		currentSize += value
	}
	if currentNode != nil && currentHour >= 0 {
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(currentNode, currentHour),
			Value: lex.EncodeOrDie(currentSize),
		}
	}
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

func (store *BytesPerHourPostgresStore) WriteRecord(record *store.Record) error {
	var nodeId []byte
	var timestamp, size int64

	lex.DecodeOrDie(record.Key, &nodeId, &timestamp)
	lex.DecodeOrDie(record.Value, &size)

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
