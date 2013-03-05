package passive

import (
	"bytes"
	"github.com/sburnett/transformer"
	"sort"
)

type IncludeNodesStore struct {
	nodes        []string
	currentIndex int
	reader       transformer.StoreSeeker
}

func IncludeNodes(reader transformer.StoreSeeker, nodes ...string) *IncludeNodesStore {
	return &IncludeNodesStore{
		nodes:        nodes,
		currentIndex: 0,
		reader:       reader,
	}
}

func (store *IncludeNodesStore) BeginReading() error {
	sort.Strings(store.nodes)
	return store.reader.BeginReading()
}

func (store *IncludeNodesStore) ReadRecord() (*transformer.LevelDbRecord, error) {
	var currentRecord *transformer.LevelDbRecord
	for {
		record, err := store.reader.ReadRecord()
		if err != nil {
			return nil, err
		}
		if record == nil {
			return nil, nil
		}
		currentRecord = record
		traceKey := DecodeTraceKey(currentRecord.Key)
		comparison := bytes.Compare(traceKey.NodeId, []byte(store.nodes[store.currentIndex]))
		if comparison == 0 {
			break
		}
		if comparison > 0 {
			store.currentIndex++
		}
		if store.currentIndex >= len(store.nodes) {
			return nil, nil
		}
		store.reader.Seek([]byte(store.nodes[store.currentIndex]))
	}
	return currentRecord, nil
}

func (store *IncludeNodesStore) EndReading() error {
	return store.reader.EndReading()
}
