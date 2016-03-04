package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/remind101/kinesis/kinesis"
)

func main() {
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	flags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	var partitionKey = flags.String("partitionKey", "", "the partition key to write to")
	flags.Parse(os.Args[2:])

	streamName := os.Args[1]

	stream := newWriter(streamName, *partitionKey)

	errCh := make(chan error, 1)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		_, err := io.Copy(stream, os.Stdin)
		errCh <- err
	}()

	var err error
	select {
	case <-c:
	case err = <-errCh:
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "err: %v", err)
		os.Exit(1)
	}

	if err = stream.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v", err)
		os.Exit(1)
	}
}

// newWriter returns an io.Writer that will ideally be able to flush to kinesis
// as fast as possible.
func newWriter(streamName, partitionKey string) *kinesis.BufferedWriter {
	return kinesis.NewFastWriter(streamName, partitionKey)
}
