package passive

import (
	"bytes"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"time"
)

type FilterTraces struct {
	NodeId           []byte
	SessionStartTime int64
	SessionEndTime   int64
}

func FilterTracesPipeline(nodeId string, sessionStartDate, sessionEndDate string, workers int) []transformer.PipelineStage {
	timeFormatString := "20060102"
	sessionStartTime, err := time.Parse(timeFormatString, sessionStartDate)
	if err != nil {
		panic(fmt.Errorf("Error parsing start date %s: %v", sessionStartDate, err))
	}
	sessionEndTime, err := time.Parse(timeFormatString, sessionEndDate)
	if err != nil {
		panic(fmt.Errorf("Error parsing end date %s: %v", sessionEndDate, err))
	}
	parameters := FilterTraces{
		NodeId:           []byte(nodeId),
		SessionStartTime: sessionStartTime.Unix() * 1000000,
		SessionEndTime:   sessionEndTime.Unix() * 1000000,
	}
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "FilterTraces",
			Transformer: transformer.MakeMapTransformer(parameters, workers),
			InputDbs:    []string{"traces"},
			OutputDbs:   []string{fmt.Sprintf("filtered-%s-%s-%s", nodeId, sessionStartDate, sessionEndDate)},
			FirstKey:    key.EncodeOrDie(parameters.NodeId),
			LastKey:     key.EncodeOrDie(append(parameters.NodeId, 1)),
			OnlyKeys:    false,
		},
	}
}

func (parameters FilterTraces) Map(inputRecord *transformer.LevelDbRecord) (outputRecord *transformer.LevelDbRecord) {
	traceKey := DecodeTraceKey(inputRecord.Key)

	if !bytes.Equal(traceKey.NodeId, parameters.NodeId) {
		return nil
	}
	if traceKey.SessionId < parameters.SessionStartTime || traceKey.SessionId > parameters.SessionEndTime {
		return nil
	}
	return inputRecord
}
