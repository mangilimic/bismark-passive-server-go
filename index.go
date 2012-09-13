package bismarkpassive

import (
	"archive/tar"
	"code.google.com/p/goprotobuf/proto"
	"compress/gzip"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func readTrace(zippedReader io.Reader) (*Trace, error) {
	unzippedReader, err := gzip.NewReader(zippedReader)
	if err != nil {
		return nil, err
	}
	return ParseTrace(unzippedReader)
}

func readTarFile(tarReader io.Reader) (traces []*Trace, traceErrors map[string]error, tarErr error) {
	traces = []*Trace{}
	traceErrors = map[string]error{}
	tr := tar.NewReader(tarReader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			tarErr = err
			break
		}
		trace, err := readTrace(tr)
		if err != nil {
			traceErrors[header.Name] = err
			continue
		}
		traces = append(traces, trace)
	}
	return
}

type indexResult struct {
	Successful    bool
	TracesIndexed int64
	TracesFailed  int64
}

func indexedChunkPath(indexPath string, trace *Trace) string {
	signature := "unanonymized"
	if trace.AnonymizationSignature != nil {
		signature = *trace.AnonymizationSignature
	}
	chunkingFactor := int32(1000)
	chunk := *trace.SequenceNumber / chunkingFactor
	return filepath.Join(indexPath, "traces", fmt.Sprintf("%s-%s", *trace.NodeId, signature), fmt.Sprintf("%d-%d", *trace.ProcessStartTimeMicroseconds, chunk))
}

func indexedTarballPath(indexPath string, tarFile string) string {
	return filepath.Join(indexPath, "tarballs", filepath.Base(tarFile))
}

func indexerLogPath(indexPath string) string {
	now := time.Now()
	return filepath.Join(indexPath, "logs", now.Format("20060102-150405"))
}

type TraceSlice []*Trace
type BySequenceNumber struct{ TraceSlice }

func (traces TraceSlice) Len() int {
	return len(traces)
}
func (traces TraceSlice) Swap(i, j int) {
	traces[i], traces[j] = traces[j], traces[i]
}
func (s BySequenceNumber) Less(i, j int) bool {
	a := s.TraceSlice[i]
	b := s.TraceSlice[j]
	if *a.NodeId < *b.NodeId {
		return true
	}
	if *a.NodeId == *b.NodeId &&
		*a.AnonymizationSignature < *b.AnonymizationSignature {
		return true
	}
	if *a.NodeId == *b.NodeId &&
		*a.AnonymizationSignature == *b.AnonymizationSignature &&
		*a.ProcessStartTimeMicroseconds < *b.ProcessStartTimeMicroseconds {
		return true
	}
	if *a.NodeId == *b.NodeId &&
		*a.AnonymizationSignature == *b.AnonymizationSignature &&
		*a.ProcessStartTimeMicroseconds == *b.ProcessStartTimeMicroseconds &&
		*a.SequenceNumber < *b.SequenceNumber {
		return true
	}
	return false
}

func readTraces(chunkPath string) *Traces {
	handle, err := os.Open(chunkPath)
	if err != nil {
		// Don't log since this isn't an error.
		return nil
	}
	defer handle.Close()
	unzippedHandle, err := gzip.NewReader(handle)
	if err != nil {
		log.Printf("Error unzipping existing chunk from %s: %s", chunkPath, err)
		return nil
	}
	defer unzippedHandle.Close()
	encoded, err := ioutil.ReadAll(unzippedHandle)
	if err != nil {
		log.Printf("Error reading existing chunk from %s: %s", chunkPath, err)
		return nil
	}
	traces := Traces{}
	if proto.Unmarshal(encoded, &traces) != nil {
		log.Printf("Error unmarshaling protobuf for %s: %s", chunkPath, err)
		return nil
	}
	return &traces
}

func traceSessionString(trace *Trace) string {
	nodeId := "<undefined>"
	if trace.NodeId != nil {
		nodeId = *trace.NodeId
	}
	anonymizationSignature := "<undefined>"
	if trace.AnonymizationSignature != nil {
		anonymizationSignature = *trace.AnonymizationSignature
	}
	sessionId := "<undefined>"
	if trace.ProcessStartTimeMicroseconds != nil {
		sessionId = fmt.Sprintf("%v", *trace.ProcessStartTimeMicroseconds)
	}
	sequenceNumber := "<undefined>"
	if trace.SequenceNumber != nil {
		sequenceNumber = fmt.Sprintf("%v", *trace.SequenceNumber)
	}
	return fmt.Sprintf("%v-%v-%v-%v", nodeId, anonymizationSignature, sessionId, sequenceNumber)
}

func mergeTraces(traces *Traces, newTraces []*Trace) {
	for _, trace := range newTraces {
		i := sort.Search(len(traces.Trace), func(i int) bool { return *traces.Trace[i].SequenceNumber >= *trace.SequenceNumber })
		if i < len(traces.Trace) && *traces.Trace[i].SequenceNumber == *trace.SequenceNumber {
			if !proto.Equal(trace, traces.Trace[i]) {
				log.Printf("Found distinct traces with duplicate sequence numbers: %v", traceSessionString(trace))
			}
			continue
		}
		traces.Trace = append(traces.Trace[:i], append([]*Trace{trace}, traces.Trace[i:]...)...)
	}
}

func writeChunk(indexPath string, chunkPath string, newTraces []*Trace) (bool, int) {
	traces := readTraces(chunkPath)
	tracesRead := 0
	if traces == nil {
		traces = &Traces{Trace: newTraces}
	} else {
		tracesRead = len(traces.Trace)
		mergeTraces(traces, newTraces)
	}
	encoded, err := proto.Marshal(traces)
	if err != nil {
		log.Printf("Error marshaling protobuf for %s: %s", chunkPath, err)
		return false, tracesRead
	}
	outputDir := filepath.Dir(chunkPath)
	if err := os.MkdirAll(outputDir, 0770); err != nil {
		log.Printf("Error on mkdir(%s): %s", outputDir, err)
		return false, tracesRead
	}
	tempDir := filepath.Join(indexPath, "tmp")
	if err := os.MkdirAll(tempDir, 0770); err != nil {
		log.Printf("Error on mkdir(%s): %s", tempDir, err)
		return false, tracesRead
	}
	chunkHandle, err := ioutil.TempFile(tempDir, "chunk")
	if err != nil {
		log.Printf("Error creating temporary file: %s", err)
		return false, tracesRead
	}
	tempChunkPath := chunkHandle.Name()
	zippedHandle := gzip.NewWriter(chunkHandle)
	if written, err := zippedHandle.Write(encoded); err != nil {
		log.Printf("Error writing %s: %s (Wrote %d bytes)", chunkPath, err, written)
		zippedHandle.Close()
		chunkHandle.Close()
		return false, tracesRead
	}
	zippedHandle.Close()
	chunkHandle.Close()
	if err := os.Rename(tempChunkPath, chunkPath); err != nil {
		log.Printf("Error on rename(%s, %s): %s", tempChunkPath, chunkPath, err)
		return false, tracesRead
	}
	return true, tracesRead
}

func initializeLogging(indexPath string) {
	logPath := indexerLogPath(indexPath)
	indexDir := filepath.Dir(logPath)
	if err := os.MkdirAll(indexDir, 0770); err != nil {
		log.Printf("Error on mkdir(%s): %s", indexDir, err)
	}
	if handle, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660); err != nil {
		log.Printf("Error opening log file %s: %s", logPath, err)
		return
	} else {
		log.SetOutput(io.MultiWriter(os.Stdout, handle))
	}
}

func IndexTraces(tarsPath string, indexPath string) {
	initializeLogging(indexPath)

	log.Printf("Scanning tarballs.")
	tarFiles, err := filepath.Glob(filepath.Join(tarsPath, "*.tar"))
	if err != nil {
		log.Println("Error enumerating tarballs: ", err)
		return
	}
	log.Printf("%d tarballs available.", len(tarFiles))

	chunksIndexed := expvar.NewInt("bismarkpassive.ChunksIndexed")
	chunksFailed := expvar.NewInt("bismarkpassive.ChunksFailed")
	chunksReread := expvar.NewInt("bismarkpassive.ChunksReread")
	tarsScanned := expvar.NewInt("bismarkpassive.TarsScanned")
	tarsIndexed := expvar.NewInt("bismarkpassive.TarsIndexed")
	tarsFailed := expvar.NewInt("bismarkpassive.TarsFailed")
	tarsSkipped := expvar.NewInt("bismarkpassive.TarsSkipped")
	tarsInvalidLink := expvar.NewInt("bismarkpassive.TarsSkippedInvalidLink")
	tarsLinked := expvar.NewInt("bismarkpassive.TarsLinked")
	tarsLinkFailed := expvar.NewInt("bismarkpassive.TarsLinkFailed")
	tracesIndexed := expvar.NewInt("bismarkpassive.TracesIndexed")
	tracesFailed := expvar.NewInt("bismarkpassive.TracesFailed")
	tracesReread := expvar.NewInt("bismarkpassive.TracesReread")

	log.Printf("Scanning index.")
	tarFilesToIndex := make([]string, 0)
	for _, tarFile := range tarFiles {
		tarsScanned.Add(int64(1))
		symlinkPath := indexedTarballPath(indexPath, tarFile)
		linkDestination, err := os.Readlink(symlinkPath)
		if err == nil {
			if linkDestination == tarFile {
				tarsSkipped.Add(int64(1))
				continue
			}
			tarsInvalidLink.Add(int64(1))
		}
		tarFilesToIndex = append(tarFilesToIndex, tarFile)
	}
	log.Printf("Indexing %d tarballs.", len(tarFilesToIndex))

	writeChunkAndSymlink := func(currentChunkPath string, currentTraces []*Trace, currentTars []string) {
		written, tracesRead := writeChunk(indexPath, currentChunkPath, currentTraces)
		if written {
			chunksIndexed.Add(int64(1))
			tracesIndexed.Add(int64(len(currentTraces)))
			tarsIndexed.Add(int64(len(currentTars)))
			for _, tarFile := range currentTars {
				symlinkPath := indexedTarballPath(indexPath, tarFile)
				symlinkDir := filepath.Dir(symlinkPath)
				if err := os.MkdirAll(symlinkDir, 0770); err != nil {
					log.Printf("Err on mkdir %s.", symlinkDir)
					tarsLinkFailed.Add(int64(1))
				}
				if err := os.Symlink(tarFile, symlinkPath); err != nil {
					log.Printf("Err creating symlink from %s to %s: %s. This tarball will probably be reprocessed later.", tarFile, symlinkPath, err)
					tarsLinkFailed.Add(int64(1))
				}
				tarsLinked.Add(int64(1))
			}
		} else {
			chunksFailed.Add(int64(1))
			tarsFailed.Add(int64(len(currentTars)))
			tracesFailed.Add(int64(len(currentTraces)))
		}
		if tracesRead > 0 {
			chunksReread.Add(int64(1))
			tracesReread.Add(int64(tracesRead))
		}
	}

	var currentChunkPath *string = nil
	currentTraces := make([]*Trace, 0)
	currentTars := make([]string, 0)
	sort.Strings(tarFilesToIndex)
	for _, tarFile := range tarFilesToIndex {
		symlinkPath := indexedTarballPath(indexPath, tarFile)
		linkDestination, err := os.Readlink(symlinkPath)
		if err == nil {
			if linkDestination == tarFile {
				tarsSkipped.Add(int64(1))
				continue
			}
			tarsInvalidLink.Add(int64(1))
		}

		handle, err := os.Open(tarFile)
		if err != nil {
			log.Printf("Error reading %s: %s\n", tarFile, err)
			tarsFailed.Add(int64(1))
			continue
		}
		traces, traceErrors, tarErr := readTarFile(handle)
		if tarErr != nil {
			log.Printf("Error indexing %s: %s\n", tarFile, tarErr)
			tarsFailed.Add(int64(1))
			handle.Close()
			continue
		}
		handle.Close()
		tracesFailed.Add(int64(len(traceErrors)))
		for traceName, traceError := range traceErrors {
			log.Printf("%s/%s: %s", tarFile, traceName, traceError)
		}

		sort.Sort(BySequenceNumber{traces})
		for _, trace := range traces {
			chunkPath := indexedChunkPath(indexPath, trace)
			if currentChunkPath == nil || chunkPath != *currentChunkPath {
				if currentChunkPath != nil {
					writeChunkAndSymlink(*currentChunkPath, currentTraces, currentTars)
				}
				currentChunkPath = &chunkPath
				currentTraces = make([]*Trace, 0)
				currentTars = make([]string, 0)
			}
			currentTraces = append(currentTraces, trace)
		}

		currentTars = append(currentTars, tarFile)
	}
	if len(currentTraces) > 0 {
		writeChunkAndSymlink(*currentChunkPath, currentTraces, currentTars)
	}
	log.Printf("Done.")
	log.Printf("Final values of exported variables:")
	expvar.Do(func(keyValue expvar.KeyValue) { log.Printf("%s: %s", keyValue.Key, keyValue.Value) })
}
