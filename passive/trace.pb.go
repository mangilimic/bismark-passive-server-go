// Code generated by protoc-gen-go.
// source: trace.proto
// DO NOT EDIT!

package passive

import proto "github.com/golang/protobuf/proto"
import json "encoding/json"
import math "math"

// Reference proto, json, and math imports to suppress error if they are not otherwise used.
var _ = proto.Marshal
var _ = &json.SyntaxError{}
var _ = math.Inf

type Trace struct {
	FileFormatVersion            *int32                 `protobuf:"varint,1,opt,name=file_format_version" json:"file_format_version,omitempty"`
	BuildId                      *string                `protobuf:"bytes,2,opt,name=build_id" json:"build_id,omitempty"`
	NodeId                       *string                `protobuf:"bytes,3,opt,name=node_id" json:"node_id,omitempty"`
	ProcessStartTimeMicroseconds *int64                 `protobuf:"varint,4,opt,name=process_start_time_microseconds" json:"process_start_time_microseconds,omitempty"`
	SequenceNumber               *int32                 `protobuf:"varint,5,opt,name=sequence_number" json:"sequence_number,omitempty"`
	TraceCreationTimestamp       *int64                 `protobuf:"varint,6,opt,name=trace_creation_timestamp" json:"trace_creation_timestamp,omitempty"`
	PcapReceived                 *uint32                `protobuf:"varint,7,opt,name=pcap_received" json:"pcap_received,omitempty"`
	PcapDropped                  *uint32                `protobuf:"varint,8,opt,name=pcap_dropped" json:"pcap_dropped,omitempty"`
	InterfaceDropped             *uint32                `protobuf:"varint,9,opt,name=interface_dropped" json:"interface_dropped,omitempty"`
	Whitelist                    []string               `protobuf:"bytes,10,rep,name=whitelist" json:"whitelist,omitempty"`
	AnonymizationSignature       *string                `protobuf:"bytes,11,opt,name=anonymization_signature" json:"anonymization_signature,omitempty"`
	PacketSeriesDropped          *uint32                `protobuf:"varint,12,opt,name=packet_series_dropped" json:"packet_series_dropped,omitempty"`
	PacketSeries                 []*PacketSeriesEntry   `protobuf:"bytes,13,rep,name=packet_series" json:"packet_series,omitempty"`
	FlowTableBaseline            *int64                 `protobuf:"varint,14,opt,name=flow_table_baseline" json:"flow_table_baseline,omitempty"`
	FlowTableSize                *uint32                `protobuf:"varint,15,opt,name=flow_table_size" json:"flow_table_size,omitempty"`
	FlowTableExpired             *int32                 `protobuf:"varint,16,opt,name=flow_table_expired" json:"flow_table_expired,omitempty"`
	FlowTableDropped             *int32                 `protobuf:"varint,17,opt,name=flow_table_dropped" json:"flow_table_dropped,omitempty"`
	FlowTableEntry               []*FlowTableEntry      `protobuf:"bytes,18,rep,name=flow_table_entry" json:"flow_table_entry,omitempty"`
	ARecordsDropped              *int32                 `protobuf:"varint,19,opt,name=a_records_dropped" json:"a_records_dropped,omitempty"`
	ARecord                      []*DnsARecord          `protobuf:"bytes,20,rep,name=a_record" json:"a_record,omitempty"`
	CnameRecordsDropped          *int32                 `protobuf:"varint,21,opt,name=cname_records_dropped" json:"cname_records_dropped,omitempty"`
	CnameRecord                  []*DnsCnameRecord      `protobuf:"bytes,22,rep,name=cname_record" json:"cname_record,omitempty"`
	AddressTableFirstId          *int32                 `protobuf:"varint,23,opt,name=address_table_first_id" json:"address_table_first_id,omitempty"`
	AddressTableSize             *int32                 `protobuf:"varint,24,opt,name=address_table_size" json:"address_table_size,omitempty"`
	AddressTableEntry            []*AddressTableEntry   `protobuf:"bytes,25,rep,name=address_table_entry" json:"address_table_entry,omitempty"`
	DroppedPacketsEntry          []*DroppedPacketsEntry `protobuf:"bytes,26,rep,name=dropped_packets_entry" json:"dropped_packets_entry,omitempty"`
	XXX_unrecognized             []byte                 `json:"-"`
}

func (this *Trace) Reset()         { *this = Trace{} }
func (this *Trace) String() string { return proto.CompactTextString(this) }
func (*Trace) ProtoMessage()       {}

func (this *Trace) GetFileFormatVersion() int32 {
	if this != nil && this.FileFormatVersion != nil {
		return *this.FileFormatVersion
	}
	return 0
}

func (this *Trace) GetBuildId() string {
	if this != nil && this.BuildId != nil {
		return *this.BuildId
	}
	return ""
}

func (this *Trace) GetNodeId() string {
	if this != nil && this.NodeId != nil {
		return *this.NodeId
	}
	return ""
}

func (this *Trace) GetProcessStartTimeMicroseconds() int64 {
	if this != nil && this.ProcessStartTimeMicroseconds != nil {
		return *this.ProcessStartTimeMicroseconds
	}
	return 0
}

func (this *Trace) GetSequenceNumber() int32 {
	if this != nil && this.SequenceNumber != nil {
		return *this.SequenceNumber
	}
	return 0
}

func (this *Trace) GetTraceCreationTimestamp() int64 {
	if this != nil && this.TraceCreationTimestamp != nil {
		return *this.TraceCreationTimestamp
	}
	return 0
}

func (this *Trace) GetPcapReceived() uint32 {
	if this != nil && this.PcapReceived != nil {
		return *this.PcapReceived
	}
	return 0
}

func (this *Trace) GetPcapDropped() uint32 {
	if this != nil && this.PcapDropped != nil {
		return *this.PcapDropped
	}
	return 0
}

func (this *Trace) GetInterfaceDropped() uint32 {
	if this != nil && this.InterfaceDropped != nil {
		return *this.InterfaceDropped
	}
	return 0
}

func (this *Trace) GetAnonymizationSignature() string {
	if this != nil && this.AnonymizationSignature != nil {
		return *this.AnonymizationSignature
	}
	return ""
}

func (this *Trace) GetPacketSeriesDropped() uint32 {
	if this != nil && this.PacketSeriesDropped != nil {
		return *this.PacketSeriesDropped
	}
	return 0
}

func (this *Trace) GetFlowTableBaseline() int64 {
	if this != nil && this.FlowTableBaseline != nil {
		return *this.FlowTableBaseline
	}
	return 0
}

func (this *Trace) GetFlowTableSize() uint32 {
	if this != nil && this.FlowTableSize != nil {
		return *this.FlowTableSize
	}
	return 0
}

func (this *Trace) GetFlowTableExpired() int32 {
	if this != nil && this.FlowTableExpired != nil {
		return *this.FlowTableExpired
	}
	return 0
}

func (this *Trace) GetFlowTableDropped() int32 {
	if this != nil && this.FlowTableDropped != nil {
		return *this.FlowTableDropped
	}
	return 0
}

func (this *Trace) GetARecordsDropped() int32 {
	if this != nil && this.ARecordsDropped != nil {
		return *this.ARecordsDropped
	}
	return 0
}

func (this *Trace) GetCnameRecordsDropped() int32 {
	if this != nil && this.CnameRecordsDropped != nil {
		return *this.CnameRecordsDropped
	}
	return 0
}

func (this *Trace) GetAddressTableFirstId() int32 {
	if this != nil && this.AddressTableFirstId != nil {
		return *this.AddressTableFirstId
	}
	return 0
}

func (this *Trace) GetAddressTableSize() int32 {
	if this != nil && this.AddressTableSize != nil {
		return *this.AddressTableSize
	}
	return 0
}

type PacketSeriesEntry struct {
	TimestampMicroseconds *int64 `protobuf:"varint,1,opt,name=timestamp_microseconds" json:"timestamp_microseconds,omitempty"`
	Size                  *int32 `protobuf:"varint,2,opt,name=size" json:"size,omitempty"`
	FlowId                *int32 `protobuf:"varint,3,opt,name=flow_id" json:"flow_id,omitempty"`
	XXX_unrecognized      []byte `json:"-"`
}

func (this *PacketSeriesEntry) Reset()         { *this = PacketSeriesEntry{} }
func (this *PacketSeriesEntry) String() string { return proto.CompactTextString(this) }
func (*PacketSeriesEntry) ProtoMessage()       {}

func (this *PacketSeriesEntry) GetTimestampMicroseconds() int64 {
	if this != nil && this.TimestampMicroseconds != nil {
		return *this.TimestampMicroseconds
	}
	return 0
}

func (this *PacketSeriesEntry) GetSize() int32 {
	if this != nil && this.Size != nil {
		return *this.Size
	}
	return 0
}

func (this *PacketSeriesEntry) GetFlowId() int32 {
	if this != nil && this.FlowId != nil {
		return *this.FlowId
	}
	return 0
}

type FlowTableEntry struct {
	FlowId                  *int32  `protobuf:"varint,1,opt,name=flow_id" json:"flow_id,omitempty"`
	SourceIpAnonymized      *bool   `protobuf:"varint,2,opt,name=source_ip_anonymized" json:"source_ip_anonymized,omitempty"`
	SourceIp                *string `protobuf:"bytes,3,opt,name=source_ip" json:"source_ip,omitempty"`
	DestinationIpAnonymized *bool   `protobuf:"varint,4,opt,name=destination_ip_anonymized" json:"destination_ip_anonymized,omitempty"`
	DestinationIp           *string `protobuf:"bytes,5,opt,name=destination_ip" json:"destination_ip,omitempty"`
	TransportProtocol       *int32  `protobuf:"varint,6,opt,name=transport_protocol" json:"transport_protocol,omitempty"`
	SourcePort              *int32  `protobuf:"varint,7,opt,name=source_port" json:"source_port,omitempty"`
	DestinationPort         *int32  `protobuf:"varint,8,opt,name=destination_port" json:"destination_port,omitempty"`
	XXX_unrecognized        []byte  `json:"-"`
}

func (this *FlowTableEntry) Reset()         { *this = FlowTableEntry{} }
func (this *FlowTableEntry) String() string { return proto.CompactTextString(this) }
func (*FlowTableEntry) ProtoMessage()       {}

func (this *FlowTableEntry) GetFlowId() int32 {
	if this != nil && this.FlowId != nil {
		return *this.FlowId
	}
	return 0
}

func (this *FlowTableEntry) GetSourceIpAnonymized() bool {
	if this != nil && this.SourceIpAnonymized != nil {
		return *this.SourceIpAnonymized
	}
	return false
}

func (this *FlowTableEntry) GetSourceIp() string {
	if this != nil && this.SourceIp != nil {
		return *this.SourceIp
	}
	return ""
}

func (this *FlowTableEntry) GetDestinationIpAnonymized() bool {
	if this != nil && this.DestinationIpAnonymized != nil {
		return *this.DestinationIpAnonymized
	}
	return false
}

func (this *FlowTableEntry) GetDestinationIp() string {
	if this != nil && this.DestinationIp != nil {
		return *this.DestinationIp
	}
	return ""
}

func (this *FlowTableEntry) GetTransportProtocol() int32 {
	if this != nil && this.TransportProtocol != nil {
		return *this.TransportProtocol
	}
	return 0
}

func (this *FlowTableEntry) GetSourcePort() int32 {
	if this != nil && this.SourcePort != nil {
		return *this.SourcePort
	}
	return 0
}

func (this *FlowTableEntry) GetDestinationPort() int32 {
	if this != nil && this.DestinationPort != nil {
		return *this.DestinationPort
	}
	return 0
}

type DnsARecord struct {
	PacketId         *int32  `protobuf:"varint,1,opt,name=packet_id" json:"packet_id,omitempty"`
	AddressId        *int32  `protobuf:"varint,2,opt,name=address_id" json:"address_id,omitempty"`
	Anonymized       *bool   `protobuf:"varint,3,opt,name=anonymized" json:"anonymized,omitempty"`
	Domain           *string `protobuf:"bytes,4,opt,name=domain" json:"domain,omitempty"`
	IpAddress        *string `protobuf:"bytes,5,opt,name=ip_address" json:"ip_address,omitempty"`
	Ttl              *int32  `protobuf:"varint,6,opt,name=ttl" json:"ttl,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *DnsARecord) Reset()         { *this = DnsARecord{} }
func (this *DnsARecord) String() string { return proto.CompactTextString(this) }
func (*DnsARecord) ProtoMessage()       {}

func (this *DnsARecord) GetPacketId() int32 {
	if this != nil && this.PacketId != nil {
		return *this.PacketId
	}
	return 0
}

func (this *DnsARecord) GetAddressId() int32 {
	if this != nil && this.AddressId != nil {
		return *this.AddressId
	}
	return 0
}

func (this *DnsARecord) GetAnonymized() bool {
	if this != nil && this.Anonymized != nil {
		return *this.Anonymized
	}
	return false
}

func (this *DnsARecord) GetDomain() string {
	if this != nil && this.Domain != nil {
		return *this.Domain
	}
	return ""
}

func (this *DnsARecord) GetIpAddress() string {
	if this != nil && this.IpAddress != nil {
		return *this.IpAddress
	}
	return ""
}

func (this *DnsARecord) GetTtl() int32 {
	if this != nil && this.Ttl != nil {
		return *this.Ttl
	}
	return 0
}

type DnsCnameRecord struct {
	PacketId         *int32  `protobuf:"varint,1,opt,name=packet_id" json:"packet_id,omitempty"`
	AddressId        *int32  `protobuf:"varint,2,opt,name=address_id" json:"address_id,omitempty"`
	DomainAnonymized *bool   `protobuf:"varint,3,opt,name=domain_anonymized" json:"domain_anonymized,omitempty"`
	Domain           *string `protobuf:"bytes,4,opt,name=domain" json:"domain,omitempty"`
	CnameAnonymized  *bool   `protobuf:"varint,5,opt,name=cname_anonymized" json:"cname_anonymized,omitempty"`
	Cname            *string `protobuf:"bytes,6,opt,name=cname" json:"cname,omitempty"`
	Ttl              *int32  `protobuf:"varint,7,opt,name=ttl" json:"ttl,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *DnsCnameRecord) Reset()         { *this = DnsCnameRecord{} }
func (this *DnsCnameRecord) String() string { return proto.CompactTextString(this) }
func (*DnsCnameRecord) ProtoMessage()       {}

func (this *DnsCnameRecord) GetPacketId() int32 {
	if this != nil && this.PacketId != nil {
		return *this.PacketId
	}
	return 0
}

func (this *DnsCnameRecord) GetAddressId() int32 {
	if this != nil && this.AddressId != nil {
		return *this.AddressId
	}
	return 0
}

func (this *DnsCnameRecord) GetDomainAnonymized() bool {
	if this != nil && this.DomainAnonymized != nil {
		return *this.DomainAnonymized
	}
	return false
}

func (this *DnsCnameRecord) GetDomain() string {
	if this != nil && this.Domain != nil {
		return *this.Domain
	}
	return ""
}

func (this *DnsCnameRecord) GetCnameAnonymized() bool {
	if this != nil && this.CnameAnonymized != nil {
		return *this.CnameAnonymized
	}
	return false
}

func (this *DnsCnameRecord) GetCname() string {
	if this != nil && this.Cname != nil {
		return *this.Cname
	}
	return ""
}

func (this *DnsCnameRecord) GetTtl() int32 {
	if this != nil && this.Ttl != nil {
		return *this.Ttl
	}
	return 0
}

type AddressTableEntry struct {
	MacAddress       *string `protobuf:"bytes,1,opt,name=mac_address" json:"mac_address,omitempty"`
	IpAddress        *string `protobuf:"bytes,2,opt,name=ip_address" json:"ip_address,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *AddressTableEntry) Reset()         { *this = AddressTableEntry{} }
func (this *AddressTableEntry) String() string { return proto.CompactTextString(this) }
func (*AddressTableEntry) ProtoMessage()       {}

func (this *AddressTableEntry) GetMacAddress() string {
	if this != nil && this.MacAddress != nil {
		return *this.MacAddress
	}
	return ""
}

func (this *AddressTableEntry) GetIpAddress() string {
	if this != nil && this.IpAddress != nil {
		return *this.IpAddress
	}
	return ""
}

type DroppedPacketsEntry struct {
	Size             *uint32 `protobuf:"varint,1,opt,name=size" json:"size,omitempty"`
	Count            *uint32 `protobuf:"varint,2,opt,name=count" json:"count,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *DroppedPacketsEntry) Reset()         { *this = DroppedPacketsEntry{} }
func (this *DroppedPacketsEntry) String() string { return proto.CompactTextString(this) }
func (*DroppedPacketsEntry) ProtoMessage()       {}

func (this *DroppedPacketsEntry) GetSize() uint32 {
	if this != nil && this.Size != nil {
		return *this.Size
	}
	return 0
}

func (this *DroppedPacketsEntry) GetCount() uint32 {
	if this != nil && this.Count != nil {
		return *this.Count
	}
	return 0
}

func init() {
}
