Radix
=====

Radix is an asynchronous Redis client for Go.
Radix was originally forked from the Tideland-rdc redis client (http://code.google.com/p/tideland-rdc/)
developed by Frank Mueller.

## Installation

    cd /path/to/radix/redis   
    make install

To run the tests:

    cd $GOROOT/src/pkg/redis &&
    gotest -v -bench=".*" && make clean && cd -

## Getting started

Creating a Client instance is done as follows:

```go
	import "redis"

	...

	c := redis.NewClient(redis.Configuration{
		Database: 0, // (default: 0)
		// Timeout in seconds
		Timeout: 10, // (default: 10)

		// Replace the line below with
		// Path: "/tmp/redis.sock" or similar for unix connections
		Address: "127.0.0.1:6379",

		//* Optional parameters
		// Password for authenticating (default: "")
		// Auth: "my_password", 
		// Size of the connection pool (default: 50)
		// PoolSize: 50, 
		// Don't try to retry on LOADING error? (default: false)
		// NoLoadingRetry: false, 
	})
	defer c.Close()
```

As Redis is mostly a single threaded database, increasing the PoolSize parameter does not usually make
much difference unless the latency to your server is very high. 
The default is set to 50 connections which should be fine for around 99.9% of cases.
However, note that each Subscription instance requires its own connection until it's closed.

Sometimes Redis may give a LOADING error when it is loading keys from the disk.
The default behaviour of Radix is to retry connecting until Redis is done with it, 
but you may wish to override this behaviour with the NoLoadingRetry parameter.

Simple blocking commands are executed using Client.Command and Client.AsyncCommand methods.
Executing multiple commands at once (pipelining) can be done with Client.MultiCommand or 
Client.Transaction methods. All of these methods return a Reply instance which contains the reply. 

Here's a simple example how to call single commands:

```go
reply := c.Command("set", "mykey", "myvalue")
if reply.Error() != nil {
	fmt.Printf("set failed: %s\n", reply.Error())
	return
}

reply = c.Command("get", "mykey")
if reply.Type() != redis.ReplyString {
	fmt.Printf("get failed: %s\n", reply.Error())
	return
}

fmt.Printf("mykey: %s\n", rep.Str())
```

The Client.Command method and alike take the command name as their first parameter, 
followed by variadic length ...interface{} parameter.
The interface{} parameters are converted into byte strings as follows:

* s []byte -> s
* s string -> []byte(s)
* s int -> strconv.Itoa(s)
* s int8 -> strconv.FormatInt(int64(s), 10)
* s int16 -> strconv.FormatInt(int64(s), 10)
* s int32 -> strconv.FormatInt(int64(s), 10)
* s int64 -> strconv.FormatInt(s, 10)
* s bool -> "1", if true, otherwise "0"

Furthermore, there is special handling for slices and maps, eg.

* []int{1,2,3} is the same as giving parameters: "1", "2", "3"
* map[string]int{"foo":1, "bar":2} is the same as giving parameters: "foo", 1, "bar", 2

For more examples on how to use multi-commands, transactions, subscriptions and more,
take a look at the example program in `example/example.go`.

## API reference

API reference is available in http://gopkgdoc.appspot.com/pkg/github.com/fzzbt/radix/redis.

Alternatively, run godoc for API reference:

	godoc -http=:8080

and point your browser to http://localhost:8080/pkg/github.com/fzzbt/radix/redis.


## HACKING

If you make contributions to the project, please follow the guidelines below:

*  Maximum line-width is 110 characters.
*  Run "gofmt -tabs=true -tabwidth=4" for any Go code before committing. 
   You may do this for all code files by running "make format".
*  Any copyright notices, etc. should not be put in any files containing program code to avoid clutter. 
   Place them in separate files instead. 
*  Avoid commenting trivial or otherwise obvious code.
*  Avoid writing fancy ascii-artsy comments. 
*  Write terse code without too much newlines or other non-essential whitespace.
*  Separate code sections with "//* My section"-styled comments.

New developers should add themselves to the lists in AUTHORS and/or CONTRIBUTORS files,
when submitting their first commit. See the CONTRIBUTORS file for details.


## Copyright and licensing

*Copyright 2011 The "radix" Authors*. See file AUTHORS and CONTRIBUTORS.  
Unless otherwise noted, the source files are distributed under the
*BSD 3-Clause License* found in the LICENSE file.
