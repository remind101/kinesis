package kinesis

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWriter_Write_Small(t *testing.T) {
	c := new(mockKinesisClient)
	w := &Writer{
		client:          c,
		partitionKey:    aws.String("key"),
		streamName:      aws.String("stream"),
		recordSizeLimit: 1000,
		putRecordsLimit: 2,
	}

	c.On("PutRecords", &kinesis.PutRecordsInput{
		StreamName: aws.String("stream"),
		Records: []*kinesis.PutRecordsRequestEntry{
			{PartitionKey: aws.String("key"), Data: []byte{'a'}},
		},
	}).Return(nil, nil)

	n, err := w.Write([]byte{'a'})
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	c.AssertExpectations(t)
}

func TestWriter_Write_Large(t *testing.T) {
	c := new(mockKinesisClient)
	w := &Writer{
		client:          c,
		partitionKey:    aws.String("key"),
		streamName:      aws.String("stream"),
		recordSizeLimit: 5,
		putRecordsLimit: 2,
	}

	c.On("PutRecords", &kinesis.PutRecordsInput{
		StreamName: aws.String("stream"),
		Records: []*kinesis.PutRecordsRequestEntry{
			{PartitionKey: aws.String("key"), Data: []byte{'a', 'b'}},
			{PartitionKey: aws.String("key"), Data: []byte{'c', 'd'}},
		},
	}).Return(nil, nil)

	n, err := w.Write([]byte{'a', 'b', 'c', 'd'})
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	c.AssertExpectations(t)
}

func TestWriter_Write_Multiple(t *testing.T) {
	c := new(mockKinesisClient)
	w := &Writer{
		client:          c,
		partitionKey:    aws.String("key"),
		streamName:      aws.String("stream"),
		recordSizeLimit: 5,
		putRecordsLimit: 2,
	}

	c.On("PutRecords", &kinesis.PutRecordsInput{
		StreamName: aws.String("stream"),
		Records: []*kinesis.PutRecordsRequestEntry{
			{PartitionKey: aws.String("key"), Data: []byte{'a', 'b'}},
			{PartitionKey: aws.String("key"), Data: []byte{'c', 'd'}},
		},
	}).Return(nil, nil)

	c.On("PutRecords", &kinesis.PutRecordsInput{
		StreamName: aws.String("stream"),
		Records: []*kinesis.PutRecordsRequestEntry{
			{PartitionKey: aws.String("key"), Data: []byte{'e'}},
		},
	}).Return(nil, nil)

	n, err := w.Write([]byte{'a', 'b', 'c', 'd', 'e'})
	assert.NoError(t, err)
	assert.Equal(t, 5, n)

	c.AssertExpectations(t)
}

type mockKinesisClient struct {
	mock.Mock
}

func (m *mockKinesisClient) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	args := m.Called(input)
	return nil, args.Error(1)
}
