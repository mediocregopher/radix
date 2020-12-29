# Radix

[![Build Status](https://travis-ci.org/mediocregopher/radix.svg)](https://travis-ci.org/mediocregopher/radix)
[![Go Reference](https://pkg.go.dev/badge/mediocregopher/radix/v4.svg)](https://pkg.go.dev/mediocregopher/radix/v4#section-documentation) (v4)
[![Go Reference](https://pkg.go.dev/badge/mediocregopher/radix/v3.svg)](https://pkg.go.dev/mediocregopher/radix/v3#section-documentation) (v3)

Radix is a full-featured [Redis][redis] client for Go. See the reference links
above for documentation and general usage examples.

## Versions

There are two major versions of radix being supported at the moment:

* v3 is the more mature version, and is still being actively supported at the
  moment.

* v4 is in beta, but is essentially stable. You can view the v4
  [CHANGELOG][v4changelog] to see what changed between the two versions. The
  biggest selling point is that connection sharing (called "implicit pipelining"
  in v3) now works with Pipeline and EvalScript, plus many other performance and
  usability enhancements.

[v4changelog]: https://github.com/mediocregopher/radix/blob/v4/CHANGELOG.md

## Features

* Standard print-like API which supports all current and future redis commands.

* Support for using an io.Reader as a command argument and writing responses to
  an io.Writer, as well as marshaling/unmarshaling command arguments from
  structs.

* Connection pooling, which takes advantage of implicit pipelining to reduce
  system calls.

* Helpers for [EVAL][eval], [SCAN][scan], and manual [pipelining][pipelining].

* Support for [pubsub][pubsub], as well as persistent pubsub wherein if a
  connection is lost a new one transparently replaces it.

* Full support for [sentinel][sentinel] and [cluster][cluster].

* Nearly all important types are interfaces, allowing for custom implementations
  of nearly anything.

## Installation and Usage

Radix always aims to support the most recent two versions of go, and is likely
to support others prior to those two.

[Module][module]-aware mode:

    go get github.com/mediocregopher/radix/v3
    // import github.com/mediocregopher/radix/v3

    go get github.com/mediocregopher/radix/v4
    // import github.com/mediocregopher/radix/v4

## Testing

    # requires a redis server running on 127.0.0.1:6379
    go test github.com/mediocregopher/radix/v3
    go test github.com/mediocregopher/radix/v4

## Benchmarks

Thanks to a huge amount of work put in by @nussjustin, and inspiration from the
[redispipe][redispipe] project and @funny-falcon, radix/v3 is significantly
faster than most redis drivers, including redigo, for normal parallel workloads,
and is pretty comparable for serial workloads.

Benchmarks can be run from the bench folder. The following results were obtained
by running the benchmarks with `-cpu` set to 32 and 64, on a 32 core machine,
with the redis server on a separate machine. See [this thread][bench_thread]
for more details.

Some of radix's results are not included below because they use a non-default
configuration.

[bench_thread]: https://github.com/mediocregopher/radix/issues/67#issuecomment-465060960


```
# go get rsc.io/benchstat
# cd bench
# go test -v -run=XXX -bench=ParallelGetSet -cpu 32 -cpu 64 -benchmem . >/tmp/radix.stat
# benchstat radix.stat
name                                   time/op
ParallelGetSet/radix/default-32        2.15µs ± 0% <--- The good stuff
ParallelGetSet/radix/default-64        2.05µs ± 0% <--- The better stuff
ParallelGetSet/redigo-32               27.9µs ± 0%
ParallelGetSet/redigo-64               28.5µs ± 0%
ParallelGetSet/redispipe-32            2.02µs ± 0%
ParallelGetSet/redispipe-64            1.71µs ± 0%

name                                   alloc/op
ParallelGetSet/radix/default-32         72.0B ± 0%
ParallelGetSet/radix/default-64         84.0B ± 0%
ParallelGetSet/redigo-32                 119B ± 0%
ParallelGetSet/redigo-64                 120B ± 0%
ParallelGetSet/redispipe-32              168B ± 0%
ParallelGetSet/redispipe-64              172B ± 0%

name                                   allocs/op
ParallelGetSet/radix/default-32          4.00 ± 0%
ParallelGetSet/radix/default-64          4.00 ± 0%
ParallelGetSet/redigo-32                 6.00 ± 0%
ParallelGetSet/redigo-64                 6.00 ± 0%
ParallelGetSet/redispipe-32              8.00 ± 0%
ParallelGetSet/redispipe-64              8.00 ± 0%
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
[redispipe]: https://github.com/joomcode/redispipe
