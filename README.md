# Radix

**WORK IN PROGRESS, NOT READY FOR USAGE YET**

[![GoDoc](https://godoc.org/github.com/mediocregopher/radix.v2/redis?status.svg)](https://godoc.org/github.com/mediocregopher/radix.v2/redis)

Radix is a minimalistic [Redis][redis] client for Go. It is broken up into
small, single-purpose packages for ease of use.

* [redis](http://godoc.org/github.com/mediocregopher/radix.v2/redis) - A wrapper around a
  single redis connection. Supports normal commands/response as well as
  pipelining.

* [pool](http://godoc.org/github.com/mediocregopher/radix.v2/pool) - a simple,
  automatically expanding/cleaning connection pool.

* extra - a sub-package containing added functionality

    * [pubsub](http://godoc.org/github.com/mediocregopher/radix.v2/extra/pubsub) - a simple
      wrapper providing convenient access to Redis Pub/Sub functionality.

    * [sentinel](http://godoc.org/github.com/mediocregopher/radix.v2/extra/sentinel) - a
      client for [redis sentinel][sentinel] which acts as a connection pool for
      a cluster of redis nodes. A sentinel client connects to a sentinel
      instance and any master redis instances that instance is monitoring. If a
      master becomes unavailable, the sentinel client will automatically start
      distributing connections from the slave chosen by the sentinel instance.

    * [cluster](http://godoc.org/github.com/mediocregopher/radix.v2/extra/cluster) - a client
      for a [redis cluster][cluster] which automatically handles interacting
      with a redis cluster, transparently handling redirects and pooling. This
      client keeps a mapping of slots to nodes internally, and automatically
      keeps it up-to-date.

## Installation

    go get github.com/mediocregopher/radix.v2/redis

## Testing

    go get github.com/stretchr/testify
    make test

The test action assumes you have a redis server listening on port 6379. It will
adiitionally bring up and tear down redis cluster nodes on ports 7000 and 7001.
You can specify the path to `redis-server` to use when setting up cluster like
so:

    make REDIS_SERVER=/path/to/redis-server test

## Why is this V2?

V1 of radix was started by [fzzy](https://github.com/fzzy) and can be found
[here](https://github.com/fzzy/radix). Some time in 2014 I took over the project
and reached a point where I couldn't make improvements that I wanted to make due
to past design decisions (mostly my own). So I've started V2, which has
redesigned some core aspects of the api and hopefully made things easier to use
and faster.

Here are the major changes since V1:

* Combining resp and redis packages

* Reply is now Resp

* List and ListBytes can no longer return nil if the response type is Nil. They
will return a bad type error in that case

* Hash is now Map

* Append is now PipeAppend, GetReply is now PipeResp

* PipelineQueueEmptyError is now ErrPipelineEmpty

## Copyright and licensing

Unless otherwise noted, the source files are distributed under the *MIT License*
found in the LICENSE.txt file.

[redis]: http://redis.io
[sentinel]: http://redis.io/topics/sentinel
[cluster]: http://redis.io/topics/cluster-spec
