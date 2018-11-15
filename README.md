# Radix

[![Build Status](https://travis-ci.org/mediocregopher/radix/v3.svg)](https://travis-ci.org/mediocregopher/radix/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/mediocregopher/radix/v3)](https://goreportcard.com/report/github.com/mediocregopher/radix/v3)
[![GoDoc](https://godoc.org/github.com/mediocregopher/radix?status.svg)][godoc]

Radix is a full-featured [Redis][redis] client for Go. See the [GoDoc][godoc]
for documentation and general usage examples.

This is the third revision of this project, the previous one has been deprecated
but can be found [here](https://github.com/mediocregopher/radix.v2).

**This project's name was recently changed from `radix.v3` to `radix`, to
account for go's new [module][module] system. As long as you are using the
latest update of your major go version (1.9.7+, 1.10.3+, 1.11+) the module-aware
go get should work correctly with the new import path.**

**I'm sorry to anyone for whom this change broke their build, I tried very hard
to not have to do it, but ultimately it was the only way that made sense for the
future. Hopefully the only thing needed to fix the breakage is to change the
import paths and re-run 'go get'.**

## Installation and Usage

[Module][module]-aware mode:

    go get github.com/mediocregopher/radix/v3
    // import github.com/mediocregopher/radix/v3

Legacy GOPATH mode:

    go get github.com/mediocregopher/radix
    // import github.com/mediocregopher/radix

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
[godoc]: https://godoc.org/github.com/mediocregopher/radix
[eval]: https://redis.io/commands/eval
[scan]: https://redis.io/commands/scan
[pipelining]: https://redis.io/topics/pipelining
[pubsub]: https://redis.io/topics/pubsub
[sentinel]: http://redis.io/topics/sentinel
[cluster]: http://redis.io/topics/cluster-spec
[module]: https://github.com/golang/go/wiki/Modules
