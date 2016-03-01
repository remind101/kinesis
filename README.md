# Kinesis for Unix

This is a small Go library and unix command for streaming to an Amazon Kinesis stream.

## Installation

```console
$ go get -u github.com/remind101/kinesis
```

## Usage

Stream a file to a kinesis stream.

```console
$ cat /var/log/syslog | kinesis logs
```

Stream to a particular partition key.

```console
$ cat /var/log/syslog | kinesis logs -p partitionKey
```

## Go Library

The Go library exposes an io.Writer implementation that writes to an Amazon Kinesis stream.

```go
import "github.com/remind101/kinesis/kinesis"

stream = bufio.NewWriter(kinesis.NewDefaultWriter("streamName", "partitionKey"))
io.Copy(stream, os.Stdin)
```
