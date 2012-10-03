Radix
=====

Radix is a Redis client for Go.


## Installation

    go get github.com/fzzbt/radix/redis

To run the tests:

    go get -u launchpad.net/gocheck
    cd $GOROOT/src/pkg/github.com/fzzbt/radix/redis
    go test -v -bench=".*"


## Status

Radix has been in development for a while already.
The API should hopefully now be stable enough for development.

Features implemented:

* Support for all commands in Redis 2.4.x, 2.6.x and unstable releases
* Pubsub support
* Pipelining support
* Simple transactions
* Asynchronous calls
* Connection pooling

Performance is decent, but don't expect Hiredis level of speeds.
Several optimization attempts have been made, 
but it seems that there is no easy way to increase performance much further.
Performance might increase in the future, 
when standard Go library is optimized.

Below are some comparative results from the `bench` program included.
Tests were run on a 4-core Intel Core Q6600 processor.

```
[fzzbt@stacker /home/fzzbt/go/src/pkg/github.com/fzzbt/radix/bench]$ ./bench -p 4 -n 1000000 set
Connections: 50, Requests: 1000000, Payload: 3 bytes, GOMAXPROCS: 4

===== SET =====
Requests per second: 50690.11036960492
Duration: 19.727714

[fzzbt@stacker /home/fzzbt/go/src/pkg/github.com/fzzbt/radix/bench]$ ./bench -p 1 -n 1000000 set
Connections: 50, Requests: 1000000, Payload: 3 bytes, GOMAXPROCS: 1

===== SET =====
Requests per second: 27662.872353327028
Duration: 36.149536

[fzzbt@stacker /home/fzzbt/go/src/pkg/github.com/fzzbt/radix/bench]$ redis-benchmark -t set -n 1000000
====== SET ======
  1000000 requests completed in 11.97 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

100.00% <= 1 milliseconds
100.00% <= 2 milliseconds
83521.26 requests per second
```

Notice how increasing GOMAXPROCS may significantly increase performance on a multi-core processor.


## Getting started

Creating a Client instance is done as follows:

```go
	import "github.com/fzzbt/radix/redis"

	...

	conf := redis.DefaultConfig()
	conf.Database = 8 // Database number 
	c = redis.NewClient(conf)
	defer c.Close()
```

Below is an example how to call simple blocking commands.
Commands can be called with the generic Call() method or with any of the shortcut methods (Set(), Get(), ...).
Executing multiple commands at once (pipelining) can be done with Client.MultiCall or 
Client.Transaction methods. These methods return a Reply instance which contains the reply. 
Asynchronous call methods are prefixed with "Async" and they return a Future instance 
instead of a Reply.

```go
reply := c.Set("mykey", "myvalue")
if reply.Err != nil {
	fmt.Println("redis:", reply.Err)
	return
}

s, err := c.Get("mykey").Str()
if err != nil {
	fmt.Printf("redis:", err)
	return
}

fmt.Println("mykey:", s)
```

Calling methods take a variadic ...interface{} as their parameter.
The interface{} parameters are converted into byte strings as follows:

* s []byte -> s
* s string -> []byte(s)
* s int -> strconv.Itoa(s)
* s int8 -> strconv.FormatInt(int64(s), 10)
* s int16 -> strconv.FormatInt(int64(s), 10)
* s int32 -> strconv.FormatInt(int64(s), 10)
* s int64 -> strconv.FormatInt(s, 10)
* s uint -> strconv.FormatUint(uint64(s), 10)
* s uint8 -> strconv.FormatUint(uint64(s), 10)
* s uint16 -> strconv.FormatUint(uint64(s), 10)
* s uint32 -> strconv.FormatUint(uint64(s), 10)
* s uint64 -> strconv.FormatUint(s, 10)
* s bool -> "1", if true, otherwise "0"

Furthermore, there is special handling for slices and maps, eg.

* []int{1,2,3} is the same as giving parameters: "1", "2", "3"
* map[string]int{"foo":1, "bar":2} is the same as giving parameters: "foo", 1, "bar", 2

Other types use reflect-based default string values.

For more examples on how to use multicalls, transactions, subscriptions and more,
take a look at the example program in `examples/example.go`.

## API reference

API reference is available in http://gopkgdoc.appspot.com/pkg/github.com/fzzbt/radix/redis.

Alternatively, run godoc for API reference:

	godoc -http=:8080

and point your browser to http://localhost:8080/pkg/github.com/fzzbt/radix/redis.


## HACKING

If you make contributions to the project, please follow the guidelines below:

*  Maximum line-width is 100 characters.
*  Run "gofmt -w -s" for all Go code before pushing your code. 
*  Avoid commenting trivial or otherwise obvious code.
*  Avoid writing fancy ascii-artsy comments. 
*  Write terse code without too much newlines or other non-essential whitespace.
*  Separate code sections with "//* My section"-styled comments.

New developers should add themselves to the list in CREDITS file,
when submitting their first commit.


## Copyright and licensing

*Copyright 2012 Juhani Åhman*.
Unless otherwise noted, the source files are distributed under the
*MIT License* found in the LICENSE file.
