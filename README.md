# Radix

Radix is a minimalistic Redis client for Go. It is broken up into the following:

* [redis](http://godoc.org/github.com/fzzy/radix/redis) - A wrapper around a
  single redis connection. Supports normal commands/response as well as
  pipelining.

    * [resp](http://godoc.org/github.com/fzzy/radix/redis/resp) - A utility
      package for encoding and decoding messages from redis

* extra - a sub-package containing added functionality

    * [pool](http://godoc.org/github.com/fzzy/radix/extra/pool) - a simple,
      automatically expanding/cleaning connection pool.

    * [pubsub](http://godoc.org/github.com/fzzy/radix/extra/pubsub) - a simple
      wrapper providing convenient access to Redis Pub/Sub functionality.

    * [sentinel](http://godoc.org/github.com/fzzy/radix/extra/sentinel) - a
      client for [redis sentinel][sentinel] which acts as a connection pool for
      a cluster of redis nodes. A sentinel client connects to a sentinel
      instance and any master redis instances that instance is monitoring. If a
      master becomes unavailable, the sentinel client will automatically start
      distributing connections from the slave chosen by the sentinel instance.

## Installation

    go get github.com/fzzy/radix/redis

## Testing

    go get -u launchpad.net/gocheck
    go test -bench=".*" ./...

## Copyright and licensing

*Copyright 2013 Juhani Åhman*.
Unless otherwise noted, the source files are distributed under the
*MIT License* found in the LICENSE file.

[sentinel]: http://redis.io/topics/sentinel
