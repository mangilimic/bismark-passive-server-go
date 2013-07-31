package passive

import (
	"github.com/sburnett/transformer/key"
	"github.com/sburnett/transformer/store"
)

func IncludeNodes(reader store.Seeker, nodes ...string) store.Seeker {
	nodesStore := store.SliceStore{}
	nodesStore.BeginWriting()
	for _, node := range nodes {
		nodesStore.WriteRecord(&store.Record{
			Key: key.EncodeOrDie(node),
		})
	}
	nodesStore.EndWriting()
	return store.NewPrefixIncludingReader(reader, &nodesStore)
}
