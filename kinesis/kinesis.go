// Package kinesis provides an io.Writer implementation that writes to an Amazon
// Kinesis stream.
package kinesis

import (
	"math"
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
