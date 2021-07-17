# Radix

[![Go v3 reference](https://pkg.go.dev/badge/github.com/mediocregopher/radix/v3.svg)](https://pkg.go.dev/github.com/mediocregopher/radix/v3#section-documentation) (v3)

[![Go v4 reference](https://pkg.go.dev/badge/github.com/mediocregopher/radix/v4.svg)](https://pkg.go.dev/github.com/mediocregopher/radix/v4#section-documentation) (v4, still in beta)

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

(When reading these it should be noted that radixv4 has not been totally
optimized for performance. It still performs well compared to v3 and other
drivers and is quite usable, but there is some work left which can be done,
specifically around its `Conn` implementation.)

Benchmarks were run in as close to a "real" environment as possible. Two GCE
instances were booted up, one hosting the redis server with 2vCPUs, the other
running the benchmarks (found in the `bench` directory) with 16vCPUs.

The benchmarks test a variety of situations against many different redis
drivers, and the results are very large. You can view them [here][bench
results]. Below are some highlights (I've tried to be fair here):

For a typical workload, which is lots of concurrent commands with relatively
small amounts of data, radix outperforms all tested drivers except
[redispipe][redispipe]:

```
BenchmarkDrivers/parallel/no_pipeline/small_kv/radixv4-64                    	17815254	      2917 ns/op	     199 B/op	       6 allocs/op
BenchmarkDrivers/parallel/no_pipeline/small_kv/radixv3-64                    	16688293	      3120 ns/op	     109 B/op	       4 allocs/op
BenchmarkDrivers/parallel/no_pipeline/small_kv/redigo-64                     	 3504063	     15092 ns/op	     168 B/op	       9 allocs/op
BenchmarkDrivers/parallel/no_pipeline/small_kv/redispipe_pause150us-64       	31668576	      1680 ns/op	     217 B/op	      11 allocs/op
BenchmarkDrivers/parallel/no_pipeline/small_kv/redispipe_pause0-64           	31149280	      1685 ns/op	     218 B/op	      11 allocs/op
BenchmarkDrivers/parallel/no_pipeline/small_kv/go-redis-64                   	 3768988	     14409 ns/op	     411 B/op	      13 allocs/op
```

The story is similar for pipelining commands concurrently (radixv3 doesn't do as
well here, because it doesn't support connection sharing for pipeline commands):

```
BenchmarkDrivers/parallel/pipeline/small_kv/radixv4-64                       	24720337	      2245 ns/op	     508 B/op	      13 allocs/op
BenchmarkDrivers/parallel/pipeline/small_kv/radixv3-64                       	 6921868	      7757 ns/op	     165 B/op	       7 allocs/op
BenchmarkDrivers/parallel/pipeline/small_kv/redigo-64                        	 6738849	      8080 ns/op	     170 B/op	       9 allocs/op
BenchmarkDrivers/parallel/pipeline/small_kv/redispipe_pause150us-64          	44479539	      1148 ns/op	     316 B/op	      12 allocs/op
BenchmarkDrivers/parallel/pipeline/small_kv/redispipe_pause0-64              	45290868	      1126 ns/op	     315 B/op	      12 allocs/op
BenchmarkDrivers/parallel/pipeline/small_kv/go-redis-64                      	 6740984	      7903 ns/op	     475 B/op	      15 allocs/op
```

For larger amounts of data being transferred the differences become less
noticeable, but both radix versions come out on top:

```
BenchmarkDrivers/parallel/no_pipeline/large_kv/radixv4-64                    	 2395707	     22766 ns/op	   12553 B/op	       4 allocs/op
BenchmarkDrivers/parallel/no_pipeline/large_kv/radixv3-64                    	 3150398	     17087 ns/op	   12745 B/op	       4 allocs/op
BenchmarkDrivers/parallel/no_pipeline/large_kv/redigo-64                     	 1593054	     34038 ns/op	   24742 B/op	       9 allocs/op
BenchmarkDrivers/parallel/no_pipeline/large_kv/redispipe_pause150us-64       	 2105118	     25085 ns/op	   16962 B/op	      11 allocs/op
BenchmarkDrivers/parallel/no_pipeline/large_kv/redispipe_pause0-64           	 2354427	     24280 ns/op	   17295 B/op	      11 allocs/op
BenchmarkDrivers/parallel/no_pipeline/large_kv/go-redis-64                   	 1519354	     35745 ns/op	   14033 B/op	      14 allocs/op
```

All results above show the high-concurrency results (`-cpu 64`). Concurrencies
of 16 and 32 are also included in the results, but didn't show anything
different.

For serial workloads, which involve a single connection performing commands
one after the other, radix is either as fast or within a couple % of the other
drivers tested. This use-case is much less common, and so when tradeoffs have
been made between parallel and serial performance radix has general leaned
towards parallel.

Serial non-pipelined:

```
BenchmarkDrivers/serial/no_pipeline/small_kv/radixv4-16 	  346915	    161493 ns/op	      67 B/op	       4 allocs/op
BenchmarkDrivers/serial/no_pipeline/small_kv/radixv3-16 	  428313	    138011 ns/op	      67 B/op	       4 allocs/op
BenchmarkDrivers/serial/no_pipeline/small_kv/redigo-16  	  416103	    134438 ns/op	     134 B/op	       8 allocs/op
BenchmarkDrivers/serial/no_pipeline/small_kv/redispipe_pause150us-16         	   86734	    635637 ns/op	     217 B/op	      11 allocs/op
BenchmarkDrivers/serial/no_pipeline/small_kv/redispipe_pause0-16             	  340320	    158732 ns/op	     216 B/op	      11 allocs/op
BenchmarkDrivers/serial/no_pipeline/small_kv/go-redis-16                     	  429703	    138854 ns/op	     408 B/op	      13 allocs/op
```

Serial pipelined:

```
BenchmarkDrivers/serial/pipeline/small_kv/radixv4-16                         	  624417	     82336 ns/op	      83 B/op	       5 allocs/op
BenchmarkDrivers/serial/pipeline/small_kv/radixv3-16                         	  784947	     68540 ns/op	     163 B/op	       7 allocs/op
BenchmarkDrivers/serial/pipeline/small_kv/redigo-16                          	  770983	     69976 ns/op	     134 B/op	       8 allocs/op
BenchmarkDrivers/serial/pipeline/small_kv/redispipe_pause150us-16            	  175623	    320512 ns/op	     312 B/op	      12 allocs/op
BenchmarkDrivers/serial/pipeline/small_kv/redispipe_pause0-16                	  642673	     82225 ns/op	     312 B/op	      12 allocs/op
BenchmarkDrivers/serial/pipeline/small_kv/go-redis-16                        	  787364	     72240 ns/op	     472 B/op	      15 allocs/op
```

Serial large values:

```
BenchmarkDrivers/serial/no_pipeline/large_kv/radixv4-16                      	  253586	    217600 ns/op	   12521 B/op	       4 allocs/op
BenchmarkDrivers/serial/no_pipeline/large_kv/radixv3-16                      	  317356	    179608 ns/op	   12717 B/op	       4 allocs/op
BenchmarkDrivers/serial/no_pipeline/large_kv/redigo-16                       	  244226	    231179 ns/op	   24704 B/op	       8 allocs/op
BenchmarkDrivers/serial/no_pipeline/large_kv/redispipe_pause150us-16         	   80174	    674066 ns/op	   13780 B/op	      11 allocs/op
BenchmarkDrivers/serial/no_pipeline/large_kv/redispipe_pause0-16             	  251810	    209890 ns/op	   13778 B/op	      11 allocs/op
BenchmarkDrivers/serial/no_pipeline/large_kv/go-redis-16                     	  236379	    225677 ns/op	   13976 B/op	      14 allocs/op
```


[bench results]: https://github.com/mediocregopher/radix/blob/v4/bench/bench_results.txt

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
