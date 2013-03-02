package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"log"
	"time"
)

func BytesPerMinutePipeline(tracesStore transformer.StoreSeeker, mappedStore transformer.Datastore, bytesPerMinuteStore transformer.StoreWriter, traceKeyRangesStore, consolidatedTraceKeyRangesStore transformer.DatastoreFull, workers int) []transformer.PipelineStage {
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
	}, TraceKeyRangesPipeline(transformer.ReadExcludingRanges(tracesStore, traceKeyRangesStore), traceKeyRangesStore, consolidatedTraceKeyRangesStore)...)
}

type BytesPerMinuteMapper transformer.Nonce

func (nonce BytesPerMinuteMapper) Do(inputRecord *transformer.LevelDbRecord, outputChan chan *transformer.LevelDbRecord) {
	traceKey := DecodeTraceKey(inputRecord.Key)

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
