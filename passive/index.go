package passive

import (
	"archive/tar"
	"compress/gzip"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"code.google.com/p/goprotobuf/proto"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

var currentTar *expvar.String
var tarBytesRead, tarsFailed, tarsIndexed, tarsSkipped, tracesFailed, tracesIndexed *expvar.Int

func init() {
	currentTar = expvar.NewString("CurrentTar")
	tarBytesRead = expvar.NewInt("TarBytesRead")
	tarsFailed = expvar.NewInt("TarsFailed")
	tarsIndexed = expvar.NewInt("TarsIndexed")
	tarsSkipped = expvar.NewInt("TarsSkipped")
	tracesFailed = expvar.NewInt("TracesFailed")
	tracesIndexed = expvar.NewInt("TracesIndexed")
}

func IndexTarballsPipeline(tarballsPath string, levelDbManager store.Manager, workers int) transformer.Pipeline {
	tarballsPattern := filepath.Join(tarballsPath, "*", "*", "*.tar.gz")
	tarnamesStore := levelDbManager.ReadingWriter("tarnames")
	tarnamesIndexedStore := levelDbManager.ReadingWriter("tarnames-indexed")
	tracesStore := levelDbManager.Writer("traces")
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:   "ScanTraceTarballs",
			Reader: store.NewGlobReader(tarballsPattern),
			Writer: tarnamesStore,
		},
		transformer.PipelineStage{
			Name:        "IndexTraces",
			Transformer: transformer.MakeMultipleOutputsGroupDoFunc(IndexTarballs, 2, workers),
			Reader:      store.NewDemuxingReader(tarnamesStore, tarnamesIndexedStore),
			Writer:      store.NewMuxingWriter(tracesStore, tarnamesIndexedStore),
		},
	}
}

func traceKey(trace *Trace) []byte {
	var anonymizationContext string
	if trace.AnonymizationSignature != nil {
		anonymizationContext = *trace.AnonymizationSignature
	}
	return lex.EncodeOrDie(
		*trace.NodeId,
		anonymizationContext,
		*trace.ProcessStartTimeMicroseconds,
		*trace.SequenceNumber)
}

func indexTarball(tarPath string, tracesChan chan *store.Record) bool {
	currentTar.Set(tarPath)
	handle, err := os.Open(tarPath)
	if err != nil {
		log.Printf("Error reading %s: %s\n", tarPath, err)
		tarsFailed.Add(int64(1))
		return false
	}
	defer handle.Close()
	fileinfo, err := handle.Stat()
	if err != nil {
		log.Printf("Error stating %s: %s\n", tarPath, err)
		tarsFailed.Add(int64(1))
		return false
	}
	tarBytesRead.Add(fileinfo.Size())
	unzippedHandle, err := gzip.NewReader(handle)
	if err != nil {
		log.Printf("Error unzipping tarball %s: %s\n", tarPath, err)
		tarsFailed.Add(int64(1))
		return false
	}
	tr := tar.NewReader(unzippedHandle)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			tarsFailed.Add(1)
			log.Printf("Error indexing %v: %v", tarPath, err)
			break
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			continue
		}
		var traceContents []byte
		if filepath.Ext(header.Name) == ".gz" {
			gzipHandle, err := gzip.NewReader(tr)
			if err != nil {
				tracesFailed.Add(1)
				log.Printf("Error gunzipping trace %s/%s: %v", tarPath, header.Name, err)
				continue
			}
			contents, err := ioutil.ReadAll(gzipHandle)
			if err != nil {
				tracesFailed.Add(1)
				log.Printf("%s/%s: %v", tarPath, header.Name, err)
				continue
			}
			gzipHandle.Close()
			traceContents = contents
		} else {
			contents, err := ioutil.ReadAll(tr)
			if err != nil {
				tracesFailed.Add(1)
				log.Printf("%s/%s: %v", tarPath, header.Name, err)
				continue
			}
			traceContents = contents
		}
		trace, err := parseTrace(traceContents)
		if err != nil {
			tracesFailed.Add(1)
			log.Printf("%s:%s: %q", tarPath, header.Name, err)
			continue
		}
		key := traceKey(trace)
		value, err := proto.Marshal(trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		tracesChan <- &store.Record{
			Key:   key,
			Value: value,
		}
		tracesIndexed.Add(int64(1))
	}
	tarsIndexed.Add(int64(1))
	return true
}

func IndexTarballs(inputRecords []*store.Record, outputChans ...chan *store.Record) {
	if len(inputRecords) != 1 {
		tarsSkipped.Add(1)
		return
	}
	if inputRecords[0].DatabaseIndex != 0 {
		return
	}

	tracesChan := outputChans[0]
	tarnamesChan := outputChans[1]

	var tarPath string
	lex.DecodeOrDie(inputRecords[0].Key, &tarPath)
	if indexTarball(tarPath, tracesChan) {
		tarnamesChan <- &store.Record{
			Key: lex.EncodeOrDie(tarPath),
		}
	}
}
