package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"io"
	"log"
)

func AvailabilityPipeline(writer io.Writer, timestamp int64, workers int) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "AvailabilityMapper",
			Transformer: transformer.TransformFunc(AvailabilityMapper),
			InputDbs:    []string{"traces"},
			OutputDbs:   []string{"availability-intervals"},
			OnlyKeys:    false,
		},
		transformer.PipelineStage{
			Name:        "AvailabilityReducer",
			Transformer: transformer.TransformFunc(AvailabilityReducer),
			InputDbs:    []string{"availability-intervals"},
			OutputDbs:   []string{"availability-nodes"},
			OnlyKeys:    false,
		},
		transformer.PipelineStage{
			Name:        "AvailabilityJson",
			Transformer: AvailabilityJson{writer, timestamp},
			InputDbs:    []string{"availability-nodes"},
			OutputDbs:   []string{},
			OnlyKeys:    false,
		},
	}
}

func AvailabilityMapper(inputChan chan *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	outputChan := outputChans[0]

	var expectedSequenceNumber int32
	currentStartTimestamp := int64(-1)
	var currentSession []byte
	var previousTrace []byte
	for record := range inputChan {
		traceKey := DecodeTraceKey(record.Key)

		session := EncodeSessionKey(&SessionKey{
			NodeId:               traceKey.NodeId,
			AnonymizationContext: traceKey.AnonymizationContext,
			SessionId:            traceKey.SessionId,
		})
		if currentSession == nil {
			currentSession = session
		}

		if !bytes.Equal(currentSession, session) {
			if previousTrace != nil {
				trace := Trace{}
				if err := proto.Unmarshal(previousTrace, &trace); err != nil {
					log.Fatalf("Error ummarshaling protocol buffer: %v", err)
				}
				if currentStartTimestamp >= 0 && trace.TraceCreationTimestamp != nil {
					currentEndTimestamp := *trace.TraceCreationTimestamp
					outputChan <- &transformer.LevelDbRecord{
						Key:   currentSession,
						Value: key.EncodeOrDie(currentStartTimestamp, currentEndTimestamp),
					}
				}
			}
			currentSession = session
			previousTrace = nil
			currentStartTimestamp = -1
			expectedSequenceNumber = 0
		}

		if traceKey.SequenceNumber != expectedSequenceNumber {
			continue
		}
		if traceKey.SequenceNumber == 0 {
			trace := Trace{}
			if err := proto.Unmarshal(record.Value, &trace); err != nil {
				log.Fatalf("Error ummarshaling protocol buffer: %v", err)
			}
			if trace.TraceCreationTimestamp == nil {
				continue
			}
			currentStartTimestamp = *trace.TraceCreationTimestamp
		}
		expectedSequenceNumber++
		previousTrace = record.Value
	}
	if previousTrace != nil {
		trace := Trace{}
		if err := proto.Unmarshal(previousTrace, &trace); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}
		if currentStartTimestamp >= 0 && trace.TraceCreationTimestamp != nil {
			currentEndTimestamp := *trace.TraceCreationTimestamp
			outputChan <- &transformer.LevelDbRecord{
				Key:   currentSession,
				Value: key.EncodeOrDie(currentStartTimestamp, currentEndTimestamp),
			}
		}
	}
}

func AvailabilityReducer(inputChan chan *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	outputChan := outputChans[0]

	var currentNode []byte
	availability := make([][]int64, 2)
	for record := range inputChan {
		sessionKey := DecodeSessionKey(record.Key)

		if !bytes.Equal(currentNode, sessionKey.NodeId) {
			if currentNode != nil {
				value, err := json.Marshal(availability)
				if err != nil {
					log.Fatalf("Error marshaling JSON: %v", err)
				}
				outputChan <- &transformer.LevelDbRecord{
					Key:   key.EncodeOrDie(currentNode),
					Value: value,
				}
			}
			currentNode = sessionKey.NodeId
			availability = make([][]int64, 2)
		}

		var startTimestamp, endTimestamp int64
		key.DecodeOrDie(record.Value, &startTimestamp, &endTimestamp)
		availability[0] = append(availability[0], startTimestamp*1000)
		availability[1] = append(availability[1], endTimestamp*1000)
	}
	if currentNode != nil {
		value, err := json.Marshal(availability)
		if err != nil {
			log.Fatalf("Error marshaling JSON: %v", err)
		}
		outputChan <- &transformer.LevelDbRecord{
			Key:   key.EncodeOrDie(currentNode),
			Value: value,
		}
	}
}

type AvailabilityJson struct {
	writer    io.Writer
	timestamp int64
}

func (t AvailabilityJson) Do(inputChan chan *transformer.LevelDbRecord, outputChans ...chan *transformer.LevelDbRecord) {
	fmt.Fprintf(t.writer, "[{")
	first := true
	for record := range inputChan {
		var nodeId string
		key.DecodeOrDie(record.Key, &nodeId)
		if first {
			first = false
		} else {
			fmt.Fprintf(t.writer, ",")
		}
		fmt.Fprintf(t.writer, "\"%s\": %s", nodeId, record.Value)
	}
	fmt.Fprintf(t.writer, "}, %d]", t.timestamp*1000)
}
