Radix
=====

Radix is an asynchronous Redis client for Go.


## Installation

    go get github.com/fzzbt/radix/redis

To run the tests:

    go get -u launchpad.net/gocheck
    cd $GOROOT/src/pkg/github.com/fzzbt/radix/redis
    go test -v -bench=".*"


## Getting started

Creating a Client instance is done as follows:

```go
	import "github.com/fzzbt/radix/redis"

	...

	c, err := redis.NewClient(redis.Configuration{
		Database: 0, // (default: 0)
		// Timeout in seconds
		Timeout: 10, // (default: 10)

		// Custom TCP/IP address or Unix path. (default: Address: "127.0.0.1:6379")
		// Address: "127.0.0.1:6379", 
		// Path: "/tmp/radix.sock"

		//* Optional parameters
		// Password for authenticating (default: "")
		// Auth: "my_password", 
		// Size of the connection pool (default: 10)
		// PoolSize: 10, 
		// Don't try to retry on LOADING error? (default: false)
		// NoLoadingRetry: false, 
	})

	if err != nil {
		fmt.Printf("NewClient failed: %s\n", err)
	}

	defer c.Close()
```

As Redis is mostly a single threaded database, increasing the PoolSize parameter does not usually make
much difference unless the latency to your server is very high. 
The default is set to 10 connections which should be fine for around 99% of cases.
However, note that each Subscription instance requires its own connection until it's closed.

Sometimes Redis may give a LOADING error when it is loading keys from the disk.
The default behaviour of Radix is to retry connecting until Redis is done with it, 
but you may wish to override this behaviour with the NoLoadingRetry parameter.

Below is an example how to call simple blocking commands.
Commands can be called with the generic Command() method or with any of the shortcut methods (Set(), Get(), ...).
Executing multiple commands at once (pipelining) can be done with Client.MultiCommand or 
Client.Transaction methods. These methods return a Reply instance which contains the reply. 
Asynchronous command method names are prefixed with "Async" and they return a Future instance 
instead of a Reply.

```go
reply := c.Set("mykey", "myvalue")
// equivalent to: reply := c.Command("SET", "mykey", "myvalue")
if reply.Error != nil {
	fmt.Printf("redis: %s\n", reply.Error)
	return
}

reply = c.Get("mykey")
if reply.Type != redis.ReplyString {
	fmt.Printf("redis: %s\n", reply.Error)
	return
}

fmt.Printf("mykey: %s\n", rep.Str())
```

Command calling methods take a variadic ...interface{} as their parameter.
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

For more examples on how to use multi-commands, transactions, subscriptions and more,
take a look at the example program in `example/example.go`.

## API reference

API reference is available in http://gopkgdoc.appspot.com/pkg/github.com/fzzbt/radix/redis.

Alternatively, run godoc for API reference:

	godoc -http=:8080

and point your browser to http://localhost:8080/pkg/github.com/fzzbt/radix/redis.


## HACKING

If you make contributions to the project, please follow the guidelines below:

*  Maximum line-width is 100 characters.
*  Run "gofmt -tabs=true -tabwidth=4 -w redis/" before pushing your code from your repository. 
*  Any copyright notices, etc. should not be put in any files containing program code to avoid clutter. 
   Place them in separate files instead. 
*  Avoid commenting trivial or otherwise obvious code.
*  Avoid writing fancy ascii-artsy comments. 
*  Write terse code without too much newlines or other non-essential whitespace.
*  Separate code sections with "//* My section"-styled comments.

New developers should add themselves to the lists in AUTHORS and/or CONTRIBUTORS files,
when submitting their first commit. See the CONTRIBUTORS file for details.


## Copyright and licensing

*Copyright 2012 The "Radix" Authors*. See file AUTHORS and CONTRIBUTORS.  
Unless otherwise noted, the source files are distributed under the
*BSD 3-Clause License* found in the LICENSE file.
