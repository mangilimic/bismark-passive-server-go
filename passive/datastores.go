package passive

import (
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
)

func IncludeNodes(reader transformer.StoreSeeker, nodes ...string) transformer.StoreSeeker {
	nodesStore := transformer.SliceStore{}
	nodesStore.BeginWriting()
	for _, node := range nodes {
		nodesStore.WriteRecord(&transformer.LevelDbRecord{
			Key: key.EncodeOrDie(node),
		})
	}
	nodesStore.EndWriting()
	return transformer.ReadIncludingPrefixes(reader, &nodesStore)
}
