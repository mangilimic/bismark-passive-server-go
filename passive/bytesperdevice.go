package passive

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"log"
	"time"
)

type FlowTimestamp struct {
	flowId int32
	timestamp int64
}

// Input: A table mapping
// node_id:anonymization_context:session_id:sequence_number to traces
// Output: Three tables:
// 1. A mapping from IP addresses to MAC addresses in each trace's address
// table. We emit one record per table entry, so there can be several records
// per trace.
// 2. A mapping from IP addresses to flow IDs for each flow in each trace's flow
// table. Again, we can emit multiple records per trace.
// 3. A mapping from flow IDs and rounded timestamps to a byte count.
func MapFromTrace(inputChan, outputChan chan *LevelDbRecord) {
	for record := range inputChan {
		key := parseKey(record.Key)  // input_table:node_id:anonymization_context:session_id:sequence_number
		if len(key) != 5 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}

		trace := Trace{}
		if err := proto.Unmarshal(record.Value, &trace); err != nil {
			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
		}

		sequenceNumberBytes, err := encodeLexicographicReverseInt32(*trace.SequenceNumber)
		if err != nil {
			log.Fatalf("Error encoding sequence number: %v", err)
		}

		for _, addressTableEntry := range trace.AddressTableEntry {
			if addressTableEntry.IpAddress == nil || addressTableEntry.MacAddress == nil {
				continue
			}
			outputChan <- &LevelDbRecord{
				Key: makeKey("ip_to_mac_and_flow", key[1], key[2], key[3], []byte(*addressTableEntry.IpAddress), sequenceNumberBytes, []byte("2")),
				Value: []byte(*addressTableEntry.MacAddress),
			}
		}

		for _, flowTableEntry := range trace.FlowTableEntry {
			if flowTableEntry.FlowId == nil {
				continue
			}
			if flowTableEntry.SourceIp != nil {
				outputChan <- &LevelDbRecord{
					Key: makeKey("ip_to_mac_and_flow", key[1], key[2], key[3], []byte(*flowTableEntry.SourceIp), sequenceNumberBytes, []byte("1")),
					Value: encodeInt64(int64(*flowTableEntry.FlowId)),
				}
			}
			if flowTableEntry.DestinationIp != nil {
				outputChan <- &LevelDbRecord{
					Key: makeKey("ip_to_mac_and_flow", key[1], key[2], key[3], []byte(*flowTableEntry.DestinationIp), sequenceNumberBytes, []byte("1")),
					Value: encodeInt64(int64(*flowTableEntry.FlowId)),
				}
			}
		}

		buckets := make(map[FlowTimestamp]int64)
		for _, packetSeriesEntry := range trace.PacketSeries {
			if packetSeriesEntry.FlowId == nil || packetSeriesEntry.TimestampMicroseconds == nil || packetSeriesEntry.Size == nil {
				continue
			}
			timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds * 1000)
			minuteTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), 0, 0, time.UTC)
			flowTimestamp := FlowTimestamp{flowId: *packetSeriesEntry.FlowId, timestamp: minuteTimestamp.Unix()}
			buckets[flowTimestamp] += int64(*packetSeriesEntry.Size)
		}
		for flowTimestamp, size := range buckets {
			flowIdBytes, err := encodeLexicographicInt32(flowTimestamp.flowId)
			if err != nil {
				log.Printf("Error encoding flow ID: %v", err)
				continue
			}
			timestampBytes, err := encodeLexicographicInt64(flowTimestamp.timestamp)
			if err != nil {
				log.Printf("Error encoding timestamp: %v", err)
				continue
			}
			outputChan <- &LevelDbRecord{
				Key: makeKey("flow_to_bytes_and_mac", key[1], key[2], key[3], flowIdBytes, sequenceNumberBytes, []byte("1"), timestampBytes),
				Value: encodeInt64(size),
			}
		}
	}
}

func JoinMacAndFlowId(inputChan, outputChan chan *LevelDbRecord) {
	var nonce int64
	var currentIpKey []byte
	var currentDestKey []byte
	var currentFlowIds [][]byte
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 7 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}

		ipKey := bytes.Join(key[1:5], []byte(":"))
		destKey := bytes.Join(key[1:4], []byte(":"))

		if currentIpKey == nil {
			currentIpKey = ipKey
		}
		if currentDestKey == nil {
			currentDestKey = destKey
		}

		if !bytes.Equal(currentIpKey, ipKey) {
			currentFlowIds = [][]byte{}
			currentIpKey = ipKey
			currentDestKey = destKey
		}

		if bytes.Equal(key[6], []byte("1")) {
			flowId, err := decodeInt64(record.Value)
			if err != nil {
				log.Fatalf("Error decoding flow id: %v", err)
			}
			encodedFlowId, err := encodeLexicographicInt32(int32(flowId))
			if err != nil {
				log.Fatalf("Error encoding flow id: %v", err)
			}
			currentFlowIds = append(currentFlowIds, bytes.Join([][]byte{encodedFlowId, key[5]}, []byte(":")))
		} else if bytes.Equal(key[6], []byte("2")) {
			currentMac := record.Value
			for _, flowIdAndSequenceNumber := range currentFlowIds {
				encodedNonce, err := encodeLexicographicInt64(nonce)
				if err != nil {
					log.Fatalf("Error encoding nonce: %v", err)
				}
				outputChan <- &LevelDbRecord{
					Key: makeKey("flow_to_bytes_and_mac", currentDestKey, flowIdAndSequenceNumber, []byte("2"), encodedNonce),
					Value: currentMac,
				}
				nonce += 1
			}
			currentFlowIds = [][]byte{}
		} else {
			log.Fatalf("Invalid tag: %v", key[6])
		}
	}
}

func JoinMacAndTimestamp(inputChan, outputChan chan *LevelDbRecord) {
	var nonce int64
	var currentFlowKey []byte
	var currentTimestamps [][]byte
	var currentSizes [][]byte
	var currentSequenceNumberKey []byte
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 7 && len(key) != 8 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}

		flowKey := bytes.Join(key[1:5], []byte(":"))
		if currentFlowKey == nil {
			currentFlowKey = flowKey
		}
		if !bytes.Equal(currentFlowKey, flowKey) {
			currentTimestamps = [][]byte{}
			currentSizes = [][]byte{}
			currentFlowKey = flowKey
		}

		if bytes.Equal(key[6], []byte("1")) {
			if currentSequenceNumberKey != nil {
				currentTimestamps = [][]byte{}
				currentSizes = [][]byte{}
				currentSequenceNumberKey = nil
			}
			currentTimestamps = append(currentTimestamps, key[7])
			currentSizes = append(currentSizes, record.Value)
		} else if bytes.Equal(key[6], []byte("2")) {
			sequenceNumberKey := bytes.Join(key[1:6], []byte(":"))
			if currentSequenceNumberKey == nil {
				currentSequenceNumberKey = sequenceNumberKey
			}
			if !bytes.Equal(sequenceNumberKey, currentSequenceNumberKey) {
				currentTimestamps = [][]byte{}
				currentSizes = [][]byte{}
				currentSequenceNumberKey = nil
			}
			currentNode := key[1]
			currentMac := record.Value
			for idx, timestamp := range currentTimestamps {
				encodedNonce, err := encodeLexicographicInt64(nonce)
				if err != nil {
					log.Fatalf("Error encoding nonce: %v", err)
				}
				outputChan <- &LevelDbRecord{
					Key: makeKey("bytes_per_device_with_nonce", currentNode, currentMac, timestamp, encodedNonce),
					Value: currentSizes[idx],
				}
				nonce++
			}
		} else {
			log.Fatalf("Invalid tag: %v", key[7])
		}
	}
}

func BytesPerDeviceReduce(inputChan, outputChan chan *LevelDbRecord) {
	var currentDestKey []byte
	var currentSize int64
	for record := range inputChan {
		key := parseKey(record.Key)
		if len(key) != 5 {
			log.Fatalf("Invalid length for key %q", record.Key)
		}

		destKey := bytes.Join(key[1:4], []byte(":"))

		if currentDestKey == nil {
			currentDestKey = destKey
		}

		if !bytes.Equal(currentDestKey, destKey) {
			outputChan <- &LevelDbRecord{
				Key: makeKey("bytes_per_device", currentDestKey),
				Value: encodeInt64(currentSize),
			}
			currentDestKey = destKey
			currentSize = 0
		}

		size, err := decodeInt64(record.Value)
		if err != nil {
			log.Fatalf("Error decoding size: %v", err)
		}
		currentSize += size
	}

	if currentDestKey != nil {
		outputChan <- &LevelDbRecord{
			Key: makeKey("bytes_per_device", currentDestKey),
			Value: encodeInt64(currentSize),
		}
	}
}
