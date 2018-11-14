# Radix

[![Build Status](https://travis-ci.org/mediocregopher/radix/v3.svg)](https://travis-ci.org/mediocregopher/radix/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/mediocregopher/radix/v3)](https://goreportcard.com/report/github.com/mediocregopher/radix/v3)
[![GoDoc](https://godoc.org/github.com/mediocregopher/radix/v3?status.svg)][godoc]

Radix is a full-featured [Redis][redis] client for Go. See the [GoDoc][godoc]
for documentation and general usage examples.

_*THIS PROJECT IS IN BETA. THE TESTS ALL PASS, BUT IT HAS NOT BEEN TESTED IN ANY
KIND OF PRODUCTION ENVIRONMENT YET. THE API IS GENERALLY STABLE, THOUGH I MAY
MAKE MINOR CHANGES STILL. ALL FEEDBACK IS APPRECIATED!*_

## Installation

    go get github.com/mediocregopher/radix/v3

Or, if you're using `go mod`:

    go mod edit -require github.com/mediocregopher/radix/v3@latest

## Testing

    # requires a redis server running on 127.0.0.1:6379
    go test github.com/mediocregopher/radix/v3

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

As of writing redigo and radix/v3 are fairly comparable, with radix being a
couple microseconds slower on average. This is in exchange for being
significantly more flexible in most use-cases, but nevertheless is an area for
future improvement.

```
# go test -v -run=XXX -bench=GetSet -benchmem >/tmp/radix.stat
# benchstat radix.stat
name                   time/op
SerialGetSet/radix     89.1µs ± 7%
SerialGetSet/redigo    87.3µs ± 7%
ParallelGetSet/radix   92.4µs ± 8%
ParallelGetSet/redigo  90.4µs ± 3%

name                   alloc/op
SerialGetSet/radix      67.0B ± 0%
SerialGetSet/redigo     86.0B ± 0%
ParallelGetSet/radix    99.0B ± 0%
ParallelGetSet/redigo    118B ± 0%

name                   allocs/op
SerialGetSet/radix       4.00 ± 0%
SerialGetSet/redigo      5.00 ± 0%
ParallelGetSet/radix     5.00 ± 0%
ParallelGetSet/redigo    6.00 ± 0%
```

## Copyright and licensing

Unless otherwise noted, the source files are distributed under the *MIT License*
found in the LICENSE.txt file.

[redis]: http://redis.io
[godoc]: https://godoc.org/github.com/mediocregopher/radix/v3
[eval]: https://redis.io/commands/eval
[scan]: https://redis.io/commands/scan
[pipelining]: https://redis.io/topics/pipelining
[pubsub]: https://redis.io/topics/pubsub
[sentinel]: http://redis.io/topics/sentinel
[cluster]: http://redis.io/topics/cluster-spec
