package main

import (
	"flag"
	"log"
	"time"

	"github.com/remind101/kinesis/kinesis"
)

func main() {
	var (
		streamName   = flag.String("stream", "", "Kinesis stream")
		partitionKey = flag.String("partitionKey", "", "Partition key")
	)
	flag.Parse()

	var total int
	kinesis.Dropped = func(p []byte) {
		log.Println(total)
		log.Fatal("dropped writes")
	}

	ch := make(chan []byte)
	go func() {
		for range time.Tick(time.Millisecond * 10) {
			ch <- make([]byte, 1024)
		}
	}()

	w := kinesis.NewFastWriter(*streamName, *partitionKey)
	for p := range ch {
		_, err := w.Write(p)
		if err != nil {
			log.Fatal(err)
		}
		total += len(p)
	}
}
