package passive

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"reflect"
)

func runBytesPerDomainPipeline(consistentRanges []*transformer.LevelDbRecord, allTraces ...map[string]Trace) {
	var stores BytesPerDomainPipelineStores

	availabilityIntervalsStore := transformer.SliceStore{}
	stores.AvailabilityIntervals = &availabilityIntervalsStore
	availabilityIntervalsStore.BeginWriting()
	for _, record := range consistentRanges {
		availabilityIntervalsStore.WriteRecord(record)
	}
	availabilityIntervalsStore.EndWriting()

	tracesStore := transformer.SliceStore{}
	stores.Traces = &tracesStore

	bytesPerDomainStore := transformer.SliceStore{}
	stores.BytesPerDomain = &bytesPerDomainStore
	bytesPerDomainPerDeviceStore := transformer.SliceStore{}
	stores.BytesPerDomainPerDevice = &bytesPerDomainPerDeviceStore

	// Fill in the remaining unitialized fields with new SliceStores.
	storesValue := reflect.ValueOf(&stores).Elem()
	modelStore := transformer.SliceStore{}
	for i := 0; i < storesValue.NumField(); i++ {
		field := storesValue.Field(i)
		if !field.IsNil() {
			continue
		}
		field.Set(reflect.New(reflect.TypeOf(modelStore)))
	}

	for _, traces := range allTraces {
		tracesStore.BeginWriting()
		for encodedKey, trace := range traces {
			encodedTrace, err := proto.Marshal(&trace)
			if err != nil {
				panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
			}
			tracesStore.WriteRecord(&transformer.LevelDbRecord{Key: []byte(encodedKey), Value: encodedTrace})
		}
		tracesStore.EndWriting()

		transformer.RunPipeline(BytesPerDomainPipeline(&stores, 1), 0)
	}

	fmt.Printf("BytesPerDomain:\n")
	bytesPerDomainStore.BeginReading()
	for {
		record, err := bytesPerDomainStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		var nodeId, domain string
		var timestamp, count int64
		key.DecodeOrDie(record.Key, &nodeId, &domain, &timestamp)
		key.DecodeOrDie(record.Value, &count)
		fmt.Printf("%s,%s,%d: %d\n", nodeId, domain, timestamp, count)
	}
	bytesPerDomainStore.EndReading()

	fmt.Printf("\nBytesPerDomainPerDevice:\n")
	bytesPerDomainPerDeviceStore.BeginReading()
	for {
		record, err := bytesPerDomainPerDeviceStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		var nodeId, macAddress, domain string
		var timestamp, count int64
		key.DecodeOrDie(record.Key, &nodeId, &macAddress, &domain, &timestamp)
		key.DecodeOrDie(record.Value, &count)
		fmt.Printf("%s,%s,%s,%d: %d\n", nodeId, macAddress, domain, timestamp, count)
	}
	bytesPerDomainPerDeviceStore.EndReading()
}

func ExampleBytesPerDomain_empty() {
	consistentRanges := []*transformer.LevelDbRecord{}
	records := map[string]Trace{}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	//
	// BytesPerDomainPerDevice:
}

func ExampleBytesPerDomain_single() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
}

func ExampleBytesPerDomain_nonZeroConstants() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(2),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(2),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(1),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(7),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(1),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(60 * 60 * 1e6), // 1:00 on Jan 1, 1970
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(7),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,3600: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,3600: 100
}

func ExampleBytesPerDomain_domains() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("foo.bar.gorp"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("prefixgorp"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("gorpsuffix"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"foo.bar.gorp",
			"bar.gorp",
			"gorp",
			"orp",
			"foobar.gorp",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,bar.gorp,0: 100
	// node0,foo.bar.gorp,0: 100
	// node0,gorp,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,bar.gorp,0: 100
	// node0,mac1,foo.bar.gorp,0: 100
	// node0,mac1,gorp,0: 100
}

func ExampleBytesPerDomain_domainsDoNotDoubleCount() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("foo.com"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("doublecounted.foo.com"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"foo.com",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,foo.com,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,foo.com,0: 100
}

func ExampleBytesPerDomain_singleCname() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(0),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 100
	// node0,domain2,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
	// node0,mac1,domain2,0: 100
}

func ExampleBytesPerDomain_singleCnameAnonymizedCname() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(true),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(true),
				PacketId:         proto.Int32(0),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain2,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain2,0: 100
}

func ExampleBytesPerDomain_singleCnameAnonymizedDomain() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(true),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(0),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
}

func ExampleBytesPerDomain_singleCnameLeaseIntersectionAFirst() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(1),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(3),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(30)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(2),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(90)),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(3),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 175
	// node0,domain2,0: 75
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 175
	// node0,mac1,domain2,0: 75
}

func ExampleBytesPerDomain_singleCnameLeaseIntersectionALonger() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(120),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(1),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(3),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(30)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(2),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(90)),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(3),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(120)),
				Size:                  proto.Int32(5),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 190
	// node0,domain2,0: 85
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 190
	// node0,mac1,domain2,0: 85
}

func ExampleBytesPerDomain_singleCnameLeaseIntersectionAOverlap() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(1),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(1),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(3),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(30)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(45)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(2),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(90)),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(3),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 185
	// node0,domain2,0: 85
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 185
	// node0,mac1,domain2,0: 85
}

func ExampleBytesPerDomain_singleCnameLeaseIntersectionCnameFirst() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(1),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(0),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(3),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(30)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(2),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(90)),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(3),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 85
	// node0,domain2,0: 75
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 85
	// node0,mac1,domain2,0: 75
}

func ExampleBytesPerDomain_singleCnameLeaseIntersectionCnameLonger() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(1),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(0),
				Ttl:              proto.Int32(120),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(3),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(30)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(2),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(90)),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(3),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(120)),
				Size:                  proto.Int32(5),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 85
	// node0,domain2,0: 85
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 85
	// node0,mac1,domain2,0: 85
}

func ExampleBytesPerDomain_singleCnameLeaseIntersectionCnameOverlap() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(1),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		CnameRecord: []*DnsCnameRecord{
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(0),
				Ttl:              proto.Int32(60),
			},
			&DnsCnameRecord{
				AddressId:        proto.Int32(0),
				Domain:           proto.String("domain2"),
				DomainAnonymized: proto.Bool(false),
				Cname:            proto.String("domain1"),
				CnameAnonymized:  proto.Bool(false),
				PacketId:         proto.Int32(1),
				Ttl:              proto.Int32(60),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(3),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(4),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(30)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(50)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(2),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(90)),
				Size:                  proto.Int32(10),
				FlowId:                proto.Int32(3),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(120)),
				Size:                  proto.Int32(5),
				FlowId:                proto.Int32(4),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 85
	// node0,domain2,0: 85
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 85
	// node0,mac1,domain2,0: 85
}

func ExampleBytesPerDomain_multiplePackets() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 150
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 150
}

func ExampleBytesPerDomain_multipleTraces() {
	trace1 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	trace2 := Trace{
		AddressTableFirstId: proto.Int32(1),
		AddressTableSize:    proto.Int32(255),
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 150
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 150
}

func ExampleBytesPerDomain_multipleHours() {
	trace1 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60 * 60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	trace2 := Trace{
		AddressTableFirstId: proto.Int32(1),
		AddressTableSize:    proto.Int32(255),
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote2"),
			},
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60 * 60)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60 * 60)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60 * 60)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(2),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 100
	// node0,domain1,3600: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
	// node0,mac1,domain1,3600: 100
}

func ExampleBytesPerDomain_multipleFlows() {
	trace1 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60 * 60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote2"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	trace2 := Trace{
		AddressTableFirstId: proto.Int32(1),
		AddressTableSize:    proto.Int32(255),
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(2),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote2"),
			},
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60 * 60)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(60 * 60)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(2),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,3600: 50
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,3600: 50
}

func ExampleBytesPerDomain_expireDnsResponses() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(5),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(10)),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(1),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(convertSecondsToMicroseconds(20)),
				Size:                  proto.Int32(25),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 125
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 125
}

func ExampleBytesPerDomain_isolateDevices() {
	trace := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
			&AddressTableEntry{
				MacAddress: proto.String("mac2"),
				IpAddress:  proto.String("local2"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
			&DnsARecord{
				AddressId:  proto.Int32(1),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(1),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
			&FlowTableEntry{
				FlowId:        proto.Int32(1),
				SourceIp:      proto.String("local2"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(1),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 200
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
	// node0,mac2,domain1,0: 100
}

func ExampleBytesPerDomain_isolateSessions() {
	trace1 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	trace2 := Trace{
		AddressTableFirstId: proto.Int32(1),
		AddressTableSize:    proto.Int32(255),
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(1), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(1), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node0", "anon0", int64(1), int32(0))): trace2,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 100
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
}

func ExampleBytesPerDomain_isolateNodes() {
	trace1 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	trace2 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain2"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
			"domain2",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
		},
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node1", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node1", "anon0", int64(0), int32(0)),
		},
	}
	records := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
		string(key.EncodeOrDie("node1", "anon0", int64(0), int32(0))): trace2,
	}

	runBytesPerDomainPipeline(consistentRanges, records)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 100
	// node1,domain2,0: 50
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 100
	// node1,mac1,domain2,0: 50
}

func ExampleBytesPerDomain_multipleRuns() {
	trace1 := Trace{
		AddressTableFirstId: proto.Int32(0),
		AddressTableSize:    proto.Int32(255),
		AddressTableEntry: []*AddressTableEntry{
			&AddressTableEntry{
				MacAddress: proto.String("mac1"),
				IpAddress:  proto.String("local1"),
			},
		},
		ARecord: []*DnsARecord{
			&DnsARecord{
				AddressId:  proto.Int32(0),
				Domain:     proto.String("domain1"),
				Anonymized: proto.Bool(false),
				PacketId:   proto.Int32(0),
				Ttl:        proto.Int32(60),
				IpAddress:  proto.String("remote1"),
			},
		},
		FlowTableEntry: []*FlowTableEntry{
			&FlowTableEntry{
				FlowId:        proto.Int32(0),
				SourceIp:      proto.String("local1"),
				DestinationIp: proto.String("remote1"),
			},
		},
		Whitelist: []string{
			"domain1",
		},
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(0),
				Size:                  proto.Int32(100),
				FlowId:                proto.Int32(0),
			},
		},
	}
	trace2 := Trace{
		AddressTableFirstId: proto.Int32(1),
		AddressTableSize:    proto.Int32(255),
		PacketSeries: []*PacketSeriesEntry{
			&PacketSeriesEntry{
				TimestampMicroseconds: proto.Int64(1),
				Size:                  proto.Int32(50),
				FlowId:                proto.Int32(0),
			},
		},
	}
	consistentRanges := []*transformer.LevelDbRecord{
		&transformer.LevelDbRecord{
			Key:   key.EncodeOrDie("node0", "anon0", int64(0), int32(0)),
			Value: key.EncodeOrDie("node0", "anon0", int64(0), int32(1)),
		},
	}
	firstRecords := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(0))): trace1,
	}
	secondRecords := map[string]Trace{
		string(key.EncodeOrDie("node0", "anon0", int64(0), int32(1))): trace2,
	}

	runBytesPerDomainPipeline(consistentRanges, firstRecords, secondRecords)

	// Output:
	// BytesPerDomain:
	// node0,domain1,0: 150
	//
	// BytesPerDomainPerDevice:
	// node0,mac1,domain1,0: 150
}
