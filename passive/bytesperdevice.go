package passive

//import (
//	"bytes"
//	"code.google.com/p/goprotobuf/proto"
//	"log"
//	"time"
//)
//
//type FlowTimestamp struct {
//	flowId    int32
//	timestamp int64
//}
//
//type UnionTag []byte
//
//var UnionFirst = UnionTag("1")
//var UnionSecond = UnionTag("2")
//var UnionThird = UnionTag("3")
//
//// Input: A table mapping
//// node_id:anonymization_context:session_id:sequence_number to traces
//// Output: Three tables:
//// 1. A mapping from IP addresses to MAC addresses in each trace's address
//// table. We emit one record per table entry, so there can be several records
//// per trace.
//// 2. A mapping from IP addresses to flow IDs for each flow in each trace's flow
//// table. Again, we can emit multiple records per trace.
//// 3. A mapping from flow IDs and rounded timestamps to a byte count.
//func MapFromTrace(inputChan, outputChan chan *LevelDbRecord) {
//	var currentSession []byte
//	var expectedSequenceNumber int32
//	for record := range inputChan {
//		key := parseKey(record.Key) // input_table:node_id:anonymization_context:session_id:sequence_number
//		if len(key) != 5 {
//			log.Fatalf("Invalid length for key %q", record.Key)
//		}
//
//		session := bytes.Join(key[1:4], []byte(":"))
//		if !bytes.Equal(currentSession, session) {
//			currentSession = session
//			expectedSequenceNumber = 0
//		}
//		sequenceNumber, err := decodeLexicographicInt32(key[4])
//		if err != nil {
//			log.Fatalf("Error decoding sequence number: %v", err)
//		}
//		if sequenceNumber != expectedSequenceNumber {
//			log.Printf("Expected sequence number %v; got %v instead", expectedSequenceNumber, sequenceNumber)
//			continue
//		}
//		expectedSequenceNumber++
//
//		trace := Trace{}
//		if err := proto.Unmarshal(record.Value, &trace); err != nil {
//			log.Fatalf("Error ummarshaling protocol buffer: %v", err)
//		}
//
//		for _, addressTableEntry := range trace.AddressTableEntry {
//			if addressTableEntry.IpAddress == nil || addressTableEntry.MacAddress == nil {
//				continue
//			}
//			outputChan <- &LevelDbRecord{
//				Key:   makeKey("ip_to_mac_and_flow", key[1], key[2], key[3], []byte(*addressTableEntry.IpAddress), key[4], UnionFirst),
//				Value: []byte(*addressTableEntry.MacAddress),
//			}
//		}
//
//		for _, flowTableEntry := range trace.FlowTableEntry {
//			if flowTableEntry.FlowId == nil {
//				continue
//			}
//			if flowTableEntry.SourceIp != nil {
//				outputChan <- &LevelDbRecord{
//					Key:   makeKey("ip_to_mac_and_flow", key[1], key[2], key[3], []byte(*flowTableEntry.SourceIp), key[4], UnionSecond),
//					Value: encodeInt64(int64(*flowTableEntry.FlowId)),
//				}
//			}
//			if flowTableEntry.DestinationIp != nil {
//				outputChan <- &LevelDbRecord{
//					Key:   makeKey("ip_to_mac_and_flow", key[1], key[2], key[3], []byte(*flowTableEntry.DestinationIp), key[4], UnionSecond),
//					Value: encodeInt64(int64(*flowTableEntry.FlowId)),
//				}
//			}
//		}
//
//		buckets := make(map[FlowTimestamp]int64)
//		for _, packetSeriesEntry := range trace.PacketSeries {
//			if packetSeriesEntry.FlowId == nil || packetSeriesEntry.TimestampMicroseconds == nil || packetSeriesEntry.Size == nil {
//				continue
//			}
//			timestamp := time.Unix(0, *packetSeriesEntry.TimestampMicroseconds*1000)
//			minuteTimestamp := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), 0, 0, time.UTC)
//			flowTimestamp := FlowTimestamp{flowId: *packetSeriesEntry.FlowId, timestamp: minuteTimestamp.Unix()}
//			buckets[flowTimestamp] += int64(*packetSeriesEntry.Size)
//		}
//		for flowTimestamp, size := range buckets {
//			flowIdBytes, err := encodeLexicographicInt32(flowTimestamp.flowId)
//			if err != nil {
//				log.Printf("Error encoding flow ID: %v", err)
//				continue
//			}
//			timestampBytes, err := encodeLexicographicInt64(flowTimestamp.timestamp)
//			if err != nil {
//				log.Printf("Error encoding timestamp: %v", err)
//				continue
//			}
//			outputChan <- &LevelDbRecord{
//				Key:   makeKey("flow_to_bytes_and_mac", key[1], key[2], key[3], flowIdBytes, key[4], UnionSecond, timestampBytes),
//				Value: encodeInt64(size),
//			}
//		}
//	}
//}
//
//func JoinMacAndFlowId(inputChan, outputChan chan *LevelDbRecord) {
//	var currentIpKey []byte
//	var currentDestKey []byte
//	var currentMac []byte
//	var nonce Nonce
//	for record := range inputChan {
//		// The key format is
//		// input_table:node_id:anonymization_context:session_id:ip:sequence_number:join_tag
//		// where join_tag is 1 if the value is a MAC address and 2 if the value
//		// is a flow ID.
//		key := parseKey(record.Key)
//		if len(key) != 7 {
//			log.Fatalf("Invalid length for key %q", record.Key)
//		}
//
//		ipKey := bytes.Join(key[1:5], []byte(":"))   // node_id:anonymization_context:session_id:ip
//		destKey := bytes.Join(key[1:4], []byte(":")) // node_id:anonymization_context:session_id
//
//		if currentIpKey == nil {
//			currentIpKey = ipKey
//		}
//		if currentDestKey == nil {
//			currentDestKey = destKey
//		}
//
//		if !bytes.Equal(currentIpKey, ipKey) {
//			currentIpKey = ipKey
//			currentDestKey = destKey
//			currentMac = nil // Keys switched to a new IP so the current MAC doesn't correspond with that IP.
//		}
//
//		if bytes.Equal(key[6], UnionFirst) {
//			currentMac = record.Value
//		} else if bytes.Equal(key[6], UnionSecond) {
//			if currentMac != nil {
//				flowId, err := decodeInt64(record.Value)
//				if err != nil {
//					log.Fatalf("Error decoding flow id: %v", err)
//				}
//				encodedFlowId, err := encodeLexicographicInt32(int32(flowId))
//				if err != nil {
//					log.Fatalf("Error encoding flow id: %v", err)
//				}
//				outputChan <- &LevelDbRecord{
//					Key:   makeKey("flow_to_bytes_and_mac", currentDestKey, encodedFlowId, key[5], UnionFirst, nonce.Next()),
//					Value: currentMac,
//				}
//			}
//		} else {
//			log.Fatalf("Invalid tag: %v", key[6])
//		}
//	}
//}
//
//func JoinMacAndTimestamp(inputChan, outputChan chan *LevelDbRecord) {
//	var nonce Nonce
//	var currentFlowKey []byte
//	var currentSequenceNumberKey []byte
//	var currentMacs [][]byte
//	for record := range inputChan {
//		// The key format is
//		// input_table:node_id:anonymization_context:session_id:flow_id:sequence_number:join_tag:timestamp
//		// where join_tag is 1 if the value is a MAC address and 2 if the value
//		// is a byte count. Timestamp is only present if join_tag is 2.
//		key := parseKey(record.Key)
//		if len(key) != 7 && len(key) != 8 {
//			log.Fatalf("Invalid length for key %q", record.Key)
//		}
//
//		flowKey := bytes.Join(key[1:5], []byte(":")) // node_id:anonymization_context:session_id:flow_id
//		if currentFlowKey == nil {
//			currentFlowKey = flowKey
//		}
//		if !bytes.Equal(currentFlowKey, flowKey) {
//			currentFlowKey = flowKey
//			currentMacs = [][]byte{} // Key switched to a new flow so we need to discover MACs for the new flow.
//		}
//
//		if bytes.Equal(key[6], UnionFirst) {
//			sequenceNumberKey := bytes.Join(key[1:6], []byte(":")) // node_id:anonymization_context:session_id:flow_id:sequence_number
//			if currentSequenceNumberKey == nil {
//				currentSequenceNumberKey = sequenceNumberKey
//			}
//			if bytes.Equal(currentSequenceNumberKey, sequenceNumberKey) {
//				// Two MACs are mapped to the same flow in the same trace.  We
//				// assume they correspond to each end of the flow and traffic
//				// should be attributed to both devices. (e.g., two devices on
//				// the LAN talking to each other.)
//				currentMacs = append(currentMacs, record.Value)
//			} else {
//				// If two MACs are mapped to the same flow in different traces
//				// then we assume they correspond to different flows sharing the
//				// same flow ID (i.e., a new flow), so we only attribute traffic
//				// to the last seen MAC.
//				currentSequenceNumberKey = sequenceNumberKey
//				currentMacs = [][]byte{record.Value}
//			}
//		} else if bytes.Equal(key[6], UnionSecond) {
//			currentNode := key[1]
//			currentTimestamp := key[7]
//			for _, currentMac := range currentMacs {
//				outputChan <- &LevelDbRecord{
//					Key:   makeKey("bytes_per_device_with_nonce", currentNode, currentMac, currentTimestamp, nonce.Next()),
//					Value: record.Value,
//				}
//			}
//		} else {
//			log.Fatalf("Invalid tag: %v", key[7])
//		}
//	}
//}
//
//func BytesPerDeviceReduce(inputChan, outputChan chan *LevelDbRecord) {
//	var currentDestKey []byte
//	var currentSize int64
//	for record := range inputChan {
//		key := parseKey(record.Key)
//		if len(key) != 5 {
//			log.Fatalf("Invalid length for key %q", record.Key)
//		}
//
//		destKey := bytes.Join(key[1:4], []byte(":"))
//
//		if currentDestKey == nil {
//			currentDestKey = destKey
//		}
//
//		if !bytes.Equal(currentDestKey, destKey) {
//			outputChan <- &LevelDbRecord{
//				Key:   makeKey("bytes_per_device", currentDestKey),
//				Value: encodeInt64(currentSize),
//			}
//			currentDestKey = destKey
//			currentSize = 0
//		}
//
//		size, err := decodeInt64(record.Value)
//		if err != nil {
//			log.Fatalf("Error decoding size: %v", err)
//		}
//		currentSize += size
//	}
//
//	if currentDestKey != nil {
//		outputChan <- &LevelDbRecord{
//			Key:   makeKey("bytes_per_device", currentDestKey),
//			Value: encodeInt64(currentSize),
//		}
//	}
//}
