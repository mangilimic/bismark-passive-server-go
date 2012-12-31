package passive

import (
	"code.google.com/p/goprotobuf/proto"
	"expvar"
	"fmt"
	"github.com/jmhodges/levigo"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"path/filepath"
	"archive/tar"
	"compress/gzip"
)

var importerBytesRead, importerBytesWritten, sessionsAdded, tarsScanned, tarsToIndex, tarsIndexed, tarsFailed, tracesIndexed, tracesFailed *expvar.Int

func init() {
	importerBytesRead = expvar.NewInt("ImporterBytesRead")
	importerBytesWritten = expvar.NewInt("ImporterBytesWritten")
	sessionsAdded = expvar.NewInt("SessionsAdded")
	tarsScanned = expvar.NewInt("TarsScanned")
	tarsToIndex = expvar.NewInt("TarsToIndex")
	tarsIndexed = expvar.NewInt("TarsIndexed")
	tarsFailed = expvar.NewInt("TarsFailed")
	tracesIndexed = expvar.NewInt("TracesIndexed")
	tracesFailed = expvar.NewInt("TracesFailed")
}

func traceKey(trace *Trace) []byte {
	var nodeId, anonymizationContext, sessionId, sequenceNumber string
	if trace.NodeId != nil {
		nodeId = *trace.NodeId
	}
	if trace.AnonymizationSignature != nil {
		anonymizationContext = *trace.AnonymizationSignature
	}
	if trace.ProcessStartTimeMicroseconds != nil {
		sessionId = fmt.Sprintf("%.20d", *trace.ProcessStartTimeMicroseconds)
	}
	if trace.SequenceNumber != nil {
		sequenceNumber = fmt.Sprintf("%.10d", *trace.SequenceNumber)
	}
	return []byte(fmt.Sprintf("trace_data:%s:%s:%s:%s", nodeId, anonymizationContext, sessionId, sequenceNumber))
}

func sessionKey(trace *Trace) string {
	var nodeId, anonymizationContext, sessionId string
	if trace.NodeId != nil {
		nodeId = *trace.NodeId
	}
	if trace.AnonymizationSignature != nil {
		anonymizationContext = *trace.AnonymizationSignature
	}
	if trace.ProcessStartTimeMicroseconds != nil {
		sessionId = fmt.Sprintf("%.20d", *trace.ProcessStartTimeMicroseconds)
	}
	return fmt.Sprintf("session_data:%s:%s:%s", nodeId, anonymizationContext, sessionId)
}

func tarballKey(tarFile string) []byte {
	return []byte(fmt.Sprintf("tarball_data:%s", filepath.Base(tarFile)))
}

func traceParseWorker(contentsChan chan []byte, traces chan *Trace, done chan bool) {
	for contents := range contentsChan {
		trace, err := parseTrace(contents)
		if err != nil {
			tracesFailed.Add(1)
			continue
		}
		traces <- trace
	}
	done <- true
}

func readFromTarballs(tarFilesToIndex []string, traceChan chan *Trace) {
	contentsChan := make(chan []byte)
	doneChan := make(chan bool)

	numTraceWorkers := 8
	for i := 0; i < numTraceWorkers; i += 1 {
		go traceParseWorker(contentsChan, traceChan, doneChan)
	}

	for _, tarFile := range tarFilesToIndex {
		handle, err := os.Open(tarFile)
		if err != nil {
			log.Printf("Error reading %s: %s\n", tarFile, err)
			tarsFailed.Add(int64(1))
			continue
		}
		fileinfo, err := handle.Stat()
		if err != nil {
			log.Printf("Error stating %s: %s\n", tarFile, err)
			tarsFailed.Add(int64(1))
			handle.Close()
			continue
		}
		importerBytesRead.Add(fileinfo.Size())
		tr := tar.NewReader(handle)
		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				tarsFailed.Add(1)
				log.Printf("Error indexing %v: %v", tarFile, err)
				break
			}
			unzippedReader, err := gzip.NewReader(tr)
			if err != nil {
				tracesFailed.Add(1)
				log.Printf("%s/%s: %v", tarFile, header.Name, err)
				continue
			}
			contents, err := ioutil.ReadAll(unzippedReader)
			if err != nil {
				tracesFailed.Add(1)
				log.Printf("%s/%s: %v", tarFile, header.Name, err)
				continue
			}
			contentsChan <- contents
		}
		tarsIndexed.Add(1)
	}
	close(contentsChan)

	for i := 0; i < numTraceWorkers; i += 1 {
		<-doneChan
	}
	close(traceChan)
}

func IndexTraces(tarsPath string, indexPath string) error {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(levigo.SnappyCompression)
	opts.SetBlockSize(1048576)
	db, err := levigo.Open(indexPath, opts)
	if err != nil {
		return fmt.Errorf("Error opening database: %v", err)
	}
	defer db.Close()
	readOpts := levigo.NewReadOptions()
	defer readOpts.Close()
	writeOpts := levigo.NewWriteOptions()
	defer writeOpts.Close()

	log.Printf("Scanning tarballs.")
	tarFiles, err := filepath.Glob(filepath.Join(tarsPath, "*.tar"))
	if err != nil {
		return fmt.Errorf("Error enumerating tarballs: ", err)
	}
	log.Printf("%d tarballs available.", len(tarFiles))

	log.Printf("Scanning index.")
	var tarFilesToIndex []string
	for _, tarFile := range tarFiles {
		tarsScanned.Add(int64(1))
		key := tarballKey(tarFile)
		if value, err := db.Get(readOpts, key); err != nil {
			return fmt.Errorf("Error looking up tarball in index: %v", err)
		} else if value != nil {
			continue
		}
		if !strings.HasPrefix(filepath.Base(tarFile), "OWC43DC79DE0F7") {
			continue
		}

		tarFilesToIndex = append(tarFilesToIndex, tarFile)
		tarsToIndex.Add(int64(1))
	}
	log.Printf("Indexing %d tarballs.", len(tarFilesToIndex))

	traces := make(chan *Trace)
	go readFromTarballs(tarFilesToIndex, traces)

	sessions := make(map[string]bool)
	for trace := range traces {
		key := traceKey(trace)
		value, err := proto.Marshal(trace)
		if err != nil {
			return fmt.Errorf("Error encoding protocol buffer: %v", err)
		}
		importerBytesWritten.Add(int64(len(value)))
		if err := db.Put(writeOpts, key, value); err != nil {
			return fmt.Errorf("Error putting trace: %v", err)
		}
		tracesIndexed.Add(1)
		sessions[sessionKey(trace)] = true
	}

	for key, _ := range sessions {
		if err := db.Put(writeOpts, []byte(key), []byte("")); err != nil {
			return fmt.Errorf("Error writing session: %v", err)
		}
		sessionsAdded.Add(1)
	}

	for _, tarFile := range tarFilesToIndex {
		key := tarballKey(tarFile)
		if err := db.Put(writeOpts, key, []byte("")); err != nil {
			return fmt.Errorf("Error marking tarball as done: %v", err)
		}
	}

	log.Printf("Done.")
	log.Printf("Final values of exported variables:")
	expvar.Do(func(keyValue expvar.KeyValue) { log.Printf("%s: %s", keyValue.Key, keyValue.Value) })

	return nil
}
