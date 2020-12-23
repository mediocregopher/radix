Changelog from v3.0.1 and up. Prior changes don't have a changelog.

# v4.0.0

* `PersistentPubSubWithOpts` has been removed.

* Rename `Action`'s `Run` method to `Perform`, and add `context.Context`
  parameter.

* Remove usage of `xerrors` package.

* Remove `Conn`'s `Encode` and `Decode` methods, add `EncodeDecode` instead.

* Remove `CmdAction`.

* Rename `resp.ErrDiscarded` to `resp.ErrConnUsable`, and change some of the
  semantics around using the error. A `resp.ErrConnUnusable` convenience
  function has been added as well.

* `resp.LenReader` now uses `int` instead of `int64` to signify length.

* `resp.Marshaler` and `resp.Unmarshaler` now take an `Opts` argument, to give
  the caller more control over things like byte pools and potentially other
  functionality in the future.

* `resp.Unmarshaler` now takes a `resp.BufferedReader`, rather than
  `*bufio.Reader`. Generally `resp.BufferedReader` will be implemented by a
  `*bufio.Reader`, but this gives move flexibility.

* Brand new `resp/resp3` package which implements the [RESP3][resp3] protocol.
  The new package features more consistent type mappings between go and redis
  and support for streaming types.

* Add `context.Context` parameter to `Client.Do`, `PubSub` methods, and
  `WithConn`.

* Add `context.Context` parameter to all `Client` and `Conn` creation functions.

* `Dial` options related to timeout have been removed in favor of context
  deadlines.

* `PubSub` has been renamed to `NewPubSubConn`.

* `Stub` and `PubSubStub` have been renamed to `NewStubConn` and
  `NewPubSubStubConn`, respectively.

* Rename `MaybeNil` to just `Maybe`, and change its semantics a bit.

* Add `MultiClient` interface which is implemented by `Sentinel` and `Cluster`,
  `Client` has been modified to be implemented only by clients which point at a
  single redis instance (`Conn` and `Pool`). Multiple methods on all affected
  client types have been modified to fit these new interfaces.

* `Cluster.NewScanner` has been replaced by `ScannerConfig.NewMulti`.

* `Conn` no longer has a `NetConn` method.

* Stop using `...opts` pattern for optional parameters across all types, and
  switch instead to a `(Config{}).New` kind of pattern.

* `Conn` is now always thread-safe. When multiple `Action`s are performed
  against a single `Conn` concurrently the `Conn` will automatically pipeline
  the `Action`'s read/writes, as appropriate.

* `Pool` has been completely rewritten to better take advantage of connection
  sharing (previously called "implicit pipelining" in v3). `EvalScript` and
  `Pipeline` now support connection sharing as well. Since most `Action`s can be
  shared on the same `Conn` the `Pool` no longer runs the risk of being depleted
  during too many concurrent `Action`s, and so no longer needs to dynamically
  create/destroy `Conn`s.

* The `trace` package has been significantly updated to reflect changes to
  `Pool` and other `Client`s.

[resp3]: https://github.com/antirez/RESP3

# v3.6.0

**New**

* Add `Typle` type, which makes unmarshaling `EXEC` and `EVAL` results easier.

* Add `PersistentPubSubErrCh`, so that asynchronous errors within
  `PersistentPubSub` can be exposed to the user.

* Add `FlatCmd` method to `EvalScript`.

* Add `StreamEntries` unmarshaler to make unmarshaling `XREAD` and `XREADGROUP`
  results easier.

**Fixes and Improvements**

* Fix wrapped errors not being handled correctly by `Cluster`. (PR #229)

* Fix `PersistentPubSub` deadlocking when a method was called after `Close`.
  (PR #230)

* Fix `StreamReader` not correctly handling the case of reading from multiple
  streams when one is empty. (PR #224)

# v3.5.2

* Improve docs for `WithConn` and `PubSubConn`.

* Fix `PubSubConn`'s `Subscribe` and `PSubscribe` methods potentially mutating
  the passed in array of strings. (Issue #217)

* Fix `StreamEntry` not properly handling unmarshaling an entry with a nil
  fields array. (PR #218)

# v3.5.1

* Add `EmptyArray` field to `MaybeNil`. (PR #211)

* Fix `Cluster` not properly re-initializing itself when the cluster goes
  completely down. (PR #209)

# v3.5.0

Huge thank you to @nussjustin for all the work he's been doing on this project,
this release is almost entirely his doing.

**New**

* Add support for `TYPE` option to `Scanner`. (PR #187)

* Add `Sentinel.DoSecondary` method. (PR #197)

* Add `DialAuthUser`, to support username+password authentication. (PR #195)

* Add `Cluster.DoSecondary` method. (PR #198)

**Fixes and Improvements**

* Fix pipeline behavior when a decode error is encountered. (PR #180)

* Fix `Reason` in `PoolConnClosed` in the case of the Pool being full. (PR #186)

* Refactor `PersistentPubSub` to be cleaner, fixing a panic in the process.
  (PR #185, Issue #184)

* Fix marshaling of nil pointers in structs. (PR #192)

* Wrap errors which get returned from pipeline decoding. (PR #191)

* Simplify and improve pipeline error handling. (PR #190)

* Dodge a `[]byte` allocation when in `StreamReader.Next`. (PR #196)

* Remove excess lock in Pool. (PR #202)


# v3.4.2

* Fix alignment for atomic values in structs (PR #171)

* Fix closing of sentinel instances while updating state (PR #173)

# v3.4.1

* Update xerrors package (PR #165)

* Have cluster Pools be closed outside of lock, to reduce contention during
  failover events (PR #168)

# v3.4.0

* Add `PersistentPubSubWithOpts` function, deprecating the old
  `PersistentPubSub` function. (PR #156)

* Make decode errors a bit more helpful. (PR #157)

* Refactor Pool to rely on its inner lock less, simplifying the code quite a bit
  and hopefully speeding up certain actions. (PR #160)

* Various documentation updates. (PR #138, Issue #162)

# v3.3.2

* Have `resp2.Error` match with a `resp.ErrDiscarded` when using `errors.As`.
  Fixes EVAL, among probably other problems. (PR #152)

# v3.3.1

* Use `xerrors` internally. (PR #113)

* Handle unmarshal errors better. Previously an unmarshaling error could leave
  the connection in an inconsistent state, because the full message wouldn't get
  completely read off the wire. After a lot of work, this has been fixed. (PR
  #127, #139, #145)

* Handle CLUSTERDOWN errors better. Upon seeing a CLUSTERDOWN, all commands will
  be delayed by a small amount of time. The delay will be stopped as soon as the
  first non-CLUSTERDOWN result is seen from the Cluster. The idea is that, if a
  failover happens, commands which are incoming will be paused long enough for
  the cluster to regain it sanity, thus minimizing the number of failed commands
  during the failover. (PR #137)

* Fix cluster redirect tracing. (PR #142)

# v3.3.0

**New**

* Add `trace` package with tracing callbacks for `Pool` and `Cluster`.
  (`Sentinel` coming soon!) (PR #100, PR #108, PR #111)

* Add `SentinelAddrs` method to `Sentinel` (PR #118)

* Add `DialUseTLS` option. (PR #104)

**Fixes and Improvements**

* Fix `NewSentinel` not handling URL AUTH parameters correctly (PR #120)

* Change `DefaultClientFunc`'s pool size from 20 to 4, on account of pipelining
  being enabled by default. (Issue #107)

* Reuse `reflect.Value` instances when unmarshaling into certain map types. (PR
  #96).

* Fix a panic in `FlatCmd`. (PR #97)

* Reuse field name `string` when unmarshaling into a struct. (PR #95)

* Reduce PubSub allocations significantly. (PR #92 + Issue #91)

* Reduce allocations in `Conn`. (PR #84)

# v3.2.3

* Optimize Scanner implementation.

* Fix bug with using types which implement resp.LenReader, encoding.TextMarshaler, and encoding.BinaryMarshaler. The encoder wasn't properly taking into account the interfaces when counting the number of elements in the message.

# v3.2.2

* Give Pool an ErrCh so that errors which happen internally may be reported to
  the user, if they care.

* Fix `PubSubConn`'s deadlock problems during Unsubscribe commands.

* Small speed optimizations in network protocol code.

# v3.2.1

* Move benchmarks to a submodule in order to clean up `go.mod` a bit.

# v3.2.0

* Add `StreamReader` type to make working with redis' new [Stream][stream]
  functionality easier.

* Make `Sentinel` properly respond to `Client` method calls. Previously it
  always created a new `Client` instance when a secondary was requested, now it
  keeps track of instances internally.

* Make default `Dial` call have a timeout for connect/read/write. At the same
  time, normalize default timeout values across the project.

* Implicitly pipeline commands in the default Pool implementation whenever
  possible. This gives a throughput increase of nearly 5x for a normal parallel
  workload.

[stream]: https://redis.io/topics/streams-intro

# v3.1.0

* Add support for marshaling/unmarshaling structs.

# v3.0.1

* Make `Stub` support `Pipeline` properly.
