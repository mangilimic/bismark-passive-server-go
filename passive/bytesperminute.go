package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"log"
	"time"
)

func BytesPerMinutePipeline(workers int) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "BytesPerMinuteMapper",
			Transformer: transformer.MakeDoTransformer(BytesPerMinuteMapper(transformer.NewNonce()), workers),
			InputDbs:    []string{"traces"},
			OutputDbs:   []string{"bytesperminute-mapped"},
			OnlyKeys:    false,
		},
		transformer.PipelineStage{
			Name:        "BytesPerMinuteReducer",
			Transformer: transformer.TransformFunc(BytesPerMinuteReducer),
			InputDbs:    []string{"bytesperminute-mapped"},
			OutputDbs:   []string{"bytesperminute"},
			OnlyKeys:    false,
		},
	}
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

func BytesPerMinuteReducer(inputChan chan *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	outputChan := outputChans[0]

	var currentSize int64
	var currentNode []byte
	currentTimestamp := int64(-1)
	for record := range inputChan {
		var node []byte
		var timestamp int64
		key.DecodeOrDie(record.Key, &node, &timestamp)

		if currentNode == nil || currentTimestamp < 0 {
			currentNode = node
			currentTimestamp = timestamp
		}
		if !bytes.Equal(node, currentNode) || timestamp != currentTimestamp {
			outputChan <- &transformer.LevelDbRecord{
				Key:   key.EncodeOrDie(currentNode, currentTimestamp),
				Value: key.EncodeOrDie(currentSize),
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
}
