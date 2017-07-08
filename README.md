# Radix

[![Build Status](https://travis-ci.org/mediocregopher/radix.v3.svg)](https://travis-ci.org/mediocregopher/radix.v3)
[![GoDoc](https://godoc.org/github.com/mediocregopher/radix.v3?status.svg)][godoc]

Radix is a full-featured [Redis][redis] client for Go. See the [GoDoc][godoc]
for documentation and general usage examples.

_*THIS PROJECT IS IN BETA. THE TESTS ALL PASS, BUT IT HAS NOT BEEN TESTED IN ANY
KIND OF PRODUCTION ENVIRONMENT YET. ALSO, THE DOCS STILL NEED WORK*_

## Installation

    go get github.com/mediocregopher/radix.v3

## Testing

    # requires a redis server running on 127.0.0.1:6379
    go test github.com/mediocregopher/radix.v3

## Features

* Standard print-like API which supports all current and future redis commands

* Support for using an io.Reader as a command argument and writing responses to
  an io.Writer.

* Connection pooling

* Helpers for [EVAL][eval], [SCAN][scan], and [pipelining][pipelining]

* Support for [pubsub][pubsub], as well as persistent pubsub wherein if a
  connection is lost a new one transparently replaces it.

* Full support for [sentinel][sentinel] and [cluster][cluster]

* Nearly all important types are interfaces, allowing for custom implementations
  of nearly anything.

## Benchmarks

As of writing redigo and radix.v3 are fairly comparable; the serial benchmarks
tend to go back and forth, but radix.v3 is consistently faster for the parallel
benchmark.

```
# go test -v -run=XXX -bench=. -benchmem -memprofilerate=1
BenchmarkSerialGetSet/radix-2              10000            147320 ns/op             196 B/op          7 allocs/op
BenchmarkSerialGetSet/redigo-2             10000            144489 ns/op             134 B/op          8 allocs/op
BenchmarkParallelGetSet/radix-2            20000             74952 ns/op             230 B/op          8 allocs/op
BenchmarkParallelGetSet/redigo-2           20000             79980 ns/op             280 B/op         12 allocs/op
```

## Copyright and licensing

Unless otherwise noted, the source files are distributed under the *MIT License*
found in the LICENSE.txt file.

[redis]: http://redis.io
[godoc]: https://godoc.org/github.com/mediocregopher/radix.v3
[eval]: https://redis.io/commands/eval
[scan]: https://redis.io/commands/scan
[pipelining]: https://redis.io/topics/pipelining
[pubsub]: https://redis.io/topics/pubsub
[sentinel]: http://redis.io/topics/sentinel
[cluster]: http://redis.io/topics/cluster-spec
