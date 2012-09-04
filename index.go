package bismarkpassive

import (
	"archive/tar"
	"compress/gzip"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"code.google.com/p/goprotobuf/proto"
)

func readTrace(zippedReader io.Reader) (*Trace, error) {
	unzippedReader, err := gzip.NewReader(zippedReader)
	if err != nil {
		return nil, err
	}
	return ParseTrace(unzippedReader)
}

func readTarFile(tarReader io.Reader) (traces map[string]*Trace, traceErrors map[string]error, tarErr error) {
	traces = map[string]*Trace{}
	traceErrors = map[string]error{}
	tr := tar.NewReader(tarReader)
	for {
		header, err := tr.Next();
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
		traces[header.Name] = trace
	}
	return
}

type indexResult struct {
	Successful bool
	TracesIndexed int64
	TracesFailed int64
}

func indexedTracePath(indexPath string, trace *Trace) string {
	signature := "unanonymized"
	if trace.AnonymizationSignature != nil {
		signature = *trace.AnonymizationSignature
	}
	sessionId := fmt.Sprint(*trace.ProcessStartTimeMicroseconds)
	sequenceNumber := fmt.Sprint(*trace.SequenceNumber)
	return filepath.Join(indexPath, "traces", *trace.NodeId, signature, sessionId, sequenceNumber)
}

func indexedTarballPath(indexPath string, tarFile string) string {
	return filepath.Join(indexPath, "tarballs", filepath.Base(tarFile))
}

func indexTraces(tarFile string, indexPath string, rateLimiter chan bool, results chan indexResult) {
	defer func() { <-rateLimiter }()

	result := indexResult{
		Successful: true,
		TracesIndexed: int64(0),
		TracesFailed: int64(0),
	}
	defer func() { results <- result }()

	handle, err := os.Open(tarFile)
	if err != nil {
		log.Printf("Error reading %s: %s\n", tarFile, err)
		result.Successful = false
		return
	}
	defer handle.Close()

	traces, traceErrors, tarErr := readTarFile(handle)
	if tarErr != nil {
		log.Printf("Error indexing %s: %s\n", tarFile, tarErr)
		result.Successful = false
		return
	}
	result.TracesFailed += int64(len(traceErrors))
	for traceName, traceError := range traceErrors {
		log.Printf("%s/%s: %s", tarFile, traceName, traceError)
	}

	for filename, trace := range traces {
		encoded, err := proto.Marshal(trace)
		if err != nil {
			log.Printf("Error marshaling protobuf for %s/%s: %s", tarFile, filename, err)
			result.TracesFailed += int64(1)
			continue
		}
		outputPath := indexedTracePath(indexPath, trace)
		outputDir := filepath.Dir(outputPath)
		if err := os.MkdirAll(outputDir, 0770); err != nil {
			log.Printf("Error on mkdir %s: %s", outputDir, err)
			result.TracesFailed += int64(1)
			continue
		}
		handle, err := os.OpenFile(outputPath, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0660)
		if err != nil {
			log.Printf("Error on open %s: %s", outputPath, err)
			result.TracesFailed += int64(1)
			continue
		}
		if written, err := handle.Write(encoded); err != nil {
			log.Printf("Error writing %s: %s (Wrote %d bytes)", outputPath, err, written)
			result.TracesFailed += int64(1)
			continue
		}
		handle.Close()
		result.TracesIndexed += int64(1)
	}
	symlinkPath := indexedTarballPath(indexPath, tarFile)
	symlinkDir := filepath.Dir(symlinkPath)
	if err := os.MkdirAll(symlinkDir, 0770); err != nil {
		log.Printf("Err on mkdir %s.", symlinkDir)
	}
	if err := os.Symlink(tarFile, symlinkPath); err != nil {
		log.Printf("Err creating symlink from %s to %s: %s. This tarball will probably be reprocessed later.", tarFile, symlinkPath, err)
	}
}

func indexTracesInParallel(tarFiles []string, indexPath string, resultsChan chan indexResult) {
	rateLimiter := make(chan bool, 16)
	for _, tarFile := range tarFiles {
		rateLimiter <- true
		go indexTraces(tarFile, indexPath, rateLimiter, resultsChan)
	}
}

func IndexTraces(tarsPath string, indexPath string) {
	tarFiles, err := filepath.Glob(filepath.Join(tarsPath, "*.tar"))
	if err != nil {
		log.Println("Error enumerating tarballs: ", err)
		return
	}
	log.Printf("Indexing %d tarballs.", len(tarFiles))

	resultsChan := make(chan indexResult)
	go indexTracesInParallel(tarFiles, indexPath, resultsChan)

	tarsIndexed := expvar.NewInt("bismarkpassive.TarsIndexed")
	tarsFailed := expvar.NewInt("bismarkpassive.TarsFailed")
	tracesIndexed := expvar.NewInt("bismarkpassive.TracesIndexed")
	tracesFailed := expvar.NewInt("bismarkpassive.TracesFailed")
	for result := range resultsChan {
		if result.Successful {
			tarsIndexed.Add(int64(1))
		} else {
			tarsFailed.Add(int64(1))
		}
		tracesIndexed.Add(result.TracesIndexed)
		tracesFailed.Add(result.TracesFailed)
	}
	log.Println("Done indexing.")
}
