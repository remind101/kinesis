// Package kinesis provides an io.Writer implementation that writes to an Amazon
// Kinesis stream.
package kinesis

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	// PutRecordsLimit is the maximum number of records allowed for a PutRecords request.
	PutRecordsLimit int = 500

	// ShardRecordsRateLimit is the maximum number of records per second per
	// shard.
	ShardRecordsRateLimit = time.Second / 1000 // 1000 records per second

	// ShardRecordsSizeLimit is the maximum data per second per shard.
	ShardRecordsSizeLimit = time.Second / (1 * 1024 * 1024) // 1MB per second

	// PutRecordsSizeLimit is the maximum allowed size per PutRecords request.
	PutRecordsSizeLimit int = 5 * 1024 * 1024 // 5MB

	// RecordSizeLimit is the maximum allowed size per record.
	RecordSizeLimit int = 1 * 1024 * 1024 // 1MB
)

// NewFastWriter returns an io.Writer that will ideally be able to flush to kinesis
// as fast as possible. It will:
//
// 1. Buffer up to 4 MB if data in memory before flushing to Kinesis.
// 2. If the buffer is not filled within 1 second, the data in the buffer will
//    be flushed.
// 4. If the calls to the Kinesis API become slow and fall behind, Writes will
//    be dropped in 4mb chunks.
func NewFastWriter(streamName, partitionKey string) *BufferedWriter {
	return newFastWriter(NewDefaultWriter(streamName, partitionKey))
}

func newFastWriter(w io.Writer) *BufferedWriter {
	// Allow up to 4 Writes to be queued, if the underlying io.Writer falls
	// behind because of slow Writes, drop it.
	dw := NewDropWriter(w, 4)

	// Buffer 4mb in memory before flushing to Kinesis and periodically
	// flush every second.
	return NewBufferedWriter(dw, 4*1024*1024, time.Second)
}

type flushWriter interface {
	io.Writer
	Flush() error
}

// BufferedWriter is a wrapper around bufio.Writer that can periodically flush
// the buffer instead of waiting for it to fill completely.
type BufferedWriter struct {
	w  flushWriter
	mu sync.Mutex // Protects the underlying bufio.Writer, since we'll be flushing in a separate goroutine.
	t  <-chan time.Time
}

// NewBufferedDefaultWriter returns a new io.Writer that buffers up to size
// bytes in memory before flushing to the Kinesis. It will also periodically
// flush the buffer if it is not full.
func NewBufferedWriter(w io.Writer, size int, flush time.Duration) *BufferedWriter {
	wr := &BufferedWriter{
		w: bufio.NewWriterSize(w, size),
		t: time.Tick(flush),
	}
	go wr.start()
	return wr
}

// start receives on the ticker and flushes.
func (w *BufferedWriter) start() {
	for range w.t {
		w.Flush()
	}
}

// Write writes p to the underlying bufio.Writer.
func (w *BufferedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

// Flush flushe the underlying bufio.Writer.
func (w *BufferedWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Flush()
}

// DropWriter is an io.Writer implementation that will drop writes when the
// underlying io.Writer is too slow to process the write. It's generally
// expected that you wrap this with a bufio.Writer.
type DropWriter struct {
	w       io.Writer
	p       chan []byte
	dropped func([]byte) // called when a Write is dropped.
	err     error
}

// NewDropWriter returns a new DropWriter implementation that will drop writes.
func NewDropWriter(w io.Writer, queue int) *DropWriter {
	wr := &DropWriter{
		w:       w,
		dropped: Dropped,
		p:       make(chan []byte, queue),
	}
	go wr.start()
	return wr
}

// start starts writing the bytes in the queue.
func (w *DropWriter) start() {
	for p := range w.p {
		if _, w.err = w.w.Write(p); w.err != nil {
			return
		}
	}
}

// Write "writes" the bytes to the underlying channel. If the channel is full,
// the packet is dropped.
func (w *DropWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	select {
	case w.p <- p:
	default:
		w.dropped(p)
	}

	return len(p), nil
}

// Dropped is the default function that is called when bytes are dropped by the
// DropWriter.
var Dropped = func(p []byte) {
	fmt.Fprintf(os.Stderr, "dropping %d bytes", len(p))
}

// NewWriter returns a new Writer instance that writes to the given stream using
// the given kinesis client.
func NewWriter(client *kinesis.Kinesis, streamName, partitionKey string) *Writer {
	return newWriter(client, streamName, partitionKey)
}

// NewDefaultWriter returns a new Writer instance that writes to the given stream. It
// uses a Kinesis client using the default credentials provided by the host.
func NewDefaultWriter(streamName, partitionKey string) *Writer {
	client := kinesis.New(session.New())
	return NewWriter(client, streamName, partitionKey)
}

// kinesisClient duck types the interface of kinesis.Kinesis that we care about.
type kinesisClient interface {
	PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

// Writer is an io.Writer that simply writes the data to a Kinesis stream. Note
// that this does NOT perform any buffering and it's expected that you wrap this
// writer with something like bufio.Writer.
type Writer struct {
	streamName, partitionKey *string

	client kinesisClient

	recordSizeLimit, putRecordsLimit int
}

// newWriter returns a new Writer instance.
func newWriter(client kinesisClient, streamName, partitionKey string) *Writer {
	return &Writer{
		client:          client,
		streamName:      aws.String(streamName),
		partitionKey:    aws.String(partitionKey),
		recordSizeLimit: RecordSizeLimit,
		putRecordsLimit: PutRecordsLimit,
	}
}

// Write splits up p into RecordSizeLimit + len(partitionKey) sized chunks of
// data, and writes them to the Kinesis stream in a single PutRecords request.
func (w *Writer) Write(p []byte) (int, error) {
	var (
		partitionKeySize = len(*w.partitionKey)

		// The maximum size of each chunk of data for each record.
		chunkSize = w.recordSizeLimit - partitionKeySize
	)

	chunks := chunk(chunkSize, p)

	var records []*kinesis.PutRecordsRequestEntry
	for _, d := range chunks {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			PartitionKey: w.partitionKey,
			Data:         d,
		})
	}

	var input *kinesis.PutRecordsInput
	for i, r := range records {
		if math.Mod(float64(i), float64(w.putRecordsLimit)) == 0 {
			if input != nil {
				if _, err := w.client.PutRecords(input); err != nil {
					return len(p), err
				}
			}

			input = &kinesis.PutRecordsInput{
				StreamName: w.streamName,
			}
		}

		input.Records = append(input.Records, r)

	}

	if input != nil {
		if _, err := w.client.PutRecords(input); err != nil {
			return len(p), err
		}
	}

	return len(p), nil
}

// chunk splits b into size sized chunks. The last chunk will be the remaining
// bytes.
func chunk(chunkSize int, b []byte) [][]byte {
	var chunks [][]byte

	var n int
	for {
		start := n * chunkSize
		end := start + chunkSize

		if end > len(b) {
			end = len(b)
		}

		chunks = append(chunks, b[start:end])

		n += 1

		if end == len(b) {
			break
		}
	}

	return chunks
}
