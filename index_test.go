package bismarkpassive

import (
	"code.google.com/p/goprotobuf/proto"
	"testing"
)

func TestMergeTraces_Disjoint(t *testing.T) {
	oldTraces := Traces{
		Trace: []*Trace{
			&Trace{
				SequenceNumber: proto.Int32(10),
			},
			&Trace{
				SequenceNumber: proto.Int32(11),
			},
		},
	}
	newTraces := []*Trace{
		&Trace{
			SequenceNumber: proto.Int32(12),
		},
	}

	mergeTraces(&oldTraces, newTraces)

	expectedTraces := Traces{
		Trace: []*Trace{
			&Trace{
				SequenceNumber: proto.Int32(10),
			},
			&Trace{
				SequenceNumber: proto.Int32(11),
			},
			&Trace{
				SequenceNumber: proto.Int32(12),
			},
		},
	}

	if !proto.Equal(&expectedTraces, &oldTraces) {
		t.Fatalf("Protocol buffers not equal:\nExpected: %s\nActual:   %s", &expectedTraces, &oldTraces)
	}
}

func TestMergeTraces_Overlap(t *testing.T) {
	oldTraces := Traces{
		Trace: []*Trace{
			&Trace{
				SequenceNumber: proto.Int32(10),
			},
			&Trace{
				SequenceNumber: proto.Int32(11),
			},
			&Trace{
				SequenceNumber: proto.Int32(12),
			},
		},
	}
	newTraces := []*Trace{
		&Trace{
			SequenceNumber: proto.Int32(11),
		},
		&Trace{
			SequenceNumber: proto.Int32(12),
			NodeId:         proto.String("additional field"),
		},
	}

	mergeTraces(&oldTraces, newTraces)

	expectedTraces := Traces{
		Trace: []*Trace{
			&Trace{
				SequenceNumber: proto.Int32(10),
			},
			&Trace{
				SequenceNumber: proto.Int32(11),
			},
			&Trace{
				SequenceNumber: proto.Int32(12),
			},
		},
	}

	if !proto.Equal(&expectedTraces, &oldTraces) {
		t.Fatalf("Protocol buffers not equal:\nExpected: %s\nActual:   %s", &expectedTraces, &oldTraces)
	}
}

func TestMergeTraces_Empty(t *testing.T) {
	oldTraces := Traces{
		Trace: []*Trace{
			&Trace{
				SequenceNumber: proto.Int32(10),
			},
			&Trace{
				SequenceNumber: proto.Int32(11),
			},
		},
	}
	newTraces := []*Trace{}

	mergeTraces(&oldTraces, newTraces)

	expectedTraces := Traces{
		Trace: []*Trace{
			&Trace{
				SequenceNumber: proto.Int32(10),
			},
			&Trace{
				SequenceNumber: proto.Int32(11),
			},
		},
	}

	if !proto.Equal(&expectedTraces, &oldTraces) {
		t.Fatalf("Protocol buffers not equal:\nExpected: %s\nActual:   %s", &expectedTraces, &oldTraces)
	}
}
