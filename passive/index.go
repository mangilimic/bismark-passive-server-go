package passive

import (
	"archive/tar"
	"code.google.com/p/goprotobuf/proto"
	"compress/gzip"
	"expvar"
	"fmt"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/key"
	"io"
	"io/ioutil"
	"log"
	"os"
)

var currentTar *expvar.String
var tarBytesRead, tarsFailed, tarsIndexed, tracesFailed, tracesIndexed *expvar.Int

func init() {
	currentTar = expvar.NewString("CurrentTar")
	tarBytesRead = expvar.NewInt("TarBytesRead")
	tarsFailed = expvar.NewInt("TarsFailed")
	tarsIndexed = expvar.NewInt("TarsIndexed")
	tracesFailed = expvar.NewInt("TracesFailed")
	tracesIndexed = expvar.NewInt("TracesIndexed")
}

func traceKey(trace *Trace) []byte {
	var anonymizationContext string
	if trace.AnonymizationSignature != nil {
		anonymizationContext = *trace.AnonymizationSignature
	}
	return key.EncodeOrDie(
		*trace.NodeId,
		anonymizationContext,
		*trace.ProcessStartTimeMicroseconds,
		*trace.SequenceNumber)
}

func indexTarball(tarPath string, tracesChan chan *transformer.LevelDbRecord) bool {
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
		contents, err := ioutil.ReadAll(tr)
		if err != nil {
			tracesFailed.Add(1)
			log.Printf("%s/%s: %v", tarPath, header.Name, err)
			continue
		}
		trace, err := parseTrace(contents)
		if err != nil {
			tracesFailed.Add(1)
			log.Printf("%s:%s: %v", tarPath, header.Name, err)
			continue
		}
		key := traceKey(trace)
		value, err := proto.Marshal(trace)
		if err != nil {
			panic(fmt.Errorf("Error encoding protocol buffer: %v", err))
		}
		tracesChan <- &transformer.LevelDbRecord{
			Key:   key,
			Value: value,
		}
		tracesIndexed.Add(int64(1))
	}
	tarsIndexed.Add(int64(1))
	return true
}

func IndexTarballs(inputRecords []*transformer.LevelDbRecord, outputChan ...chan *transformer.LevelDbRecord) {
	if len(inputRecords) != 1 || inputRecords[0].DatabaseIndex != 0 {
		return
	}

	tracesChan := outputChan[0]
	tarnamesChan := outputChan[1]

	var tarPath string
	key.DecodeOrDie(inputRecords[0].Key, &tarPath)
	if indexTarball(tarPath, tracesChan) {
		tarnamesChan <- &transformer.LevelDbRecord{
			Key: key.EncodeOrDie(tarPath),
		}
	}
}
