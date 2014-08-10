radix-extra
===========

Extra functionality built around the [radix][radix] redis client. Here's the doc
api links to available sub-packages:

* [pool](http://godoc.org/github.com/fzzy/radix/extra/pool) - a simple,
  automatically expanding/cleaning connection pool.

* [pubsub](http://godoc.org/github.com/fzzy/radix/extra/pubsub) - a simple
  wrapper providing convenient access to Redis Pub/Sub functionality.

* [sentinel](http://godoc.org/github.com/fzzy/radix/extra/sentinel) - a client
  for [redis sentinel][sentinel] which acts as a connection pool for a cluster
  of redis nodes. A sentinel client connects to a sentinel instance and any
  master redis instances that instance is monitoring. If a master becomes
  unavailable, the sentinel client will automatically start distributing
  connections from the slave chosen by the sentinel instance.

[radix]: https://github.com/fzzy/radix
[sentinel]: http://redis.io/topics/sentinel
