package redis

import (
	"flag"
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

var rd *Client
var conf Config

type TI interface {
	Fatalf(string, ...interface{})
}

func setUpTest(c TI) {
	rd = NewClient(conf)
	r := rd.Flushall()
	if r.Err != nil {
		c.Fatalf("setUp FLUSHALL failed: %s", r.Err)
	}
}

func tearDownTest(c TI) {
	r := rd.Flushall()
	if r.Err != nil {
		c.Fatalf("tearDown FLUSHALL failed: %s", r.Err)
	}

	rd.Close()
}

//* Tests
type S struct{}
type Long struct{}
type Utils struct{}

var long = flag.Bool("long", false, "Include long running tests")

func init() {
	conf = DefaultConfig()
	conf.Network = "tcp"
	conf.Address = "127.0.0.1:6379"
	conf.Database = 8
	conf.Timeout = time.Duration(10) * time.Second

	Suite(&S{})
	Suite(&Long{})
	Suite(&Utils{})
}

func (s *Long) SetUpSuite(c *C) {
	if !*long {
		c.Skip("-long not provided")
	}
}

func (s *S) SetUpTest(c *C) {
	setUpTest(c)
}

func (s *S) TearDownTest(c *C) {
	tearDownTest(c)
}

func (s *Long) SetUpTest(c *C) {
	setUpTest(c)
}

func (s *Long) TearDownTest(c *C) {
	tearDownTest(c)
}

// Test connection calls.
func (s *S) TestConnection(c *C) {
	v, _ := rd.Echo("Hello, World!").Str()
	c.Check(v, Equals, "Hello, World!")
	v, _ = rd.Ping().Str()
	c.Check(v, Equals, "PONG")
}

// Test single return value calls.
func (s *S) TestSimpleValue(c *C) {
	// Simple value calls.
	rd.Set("simple:string", "Hello,")
	rd.Append("simple:string", " World!")
	vs, _ := rd.Get("simple:string").Str()
	c.Check(vs, Equals, "Hello, World!")

	rd.Set("simple:int", 10)
	vy, _ := rd.Incr("simple:int").Int()
	c.Check(vy, Equals, 11)

	rd.Setbit("simple:bit", 0, true)
	rd.Setbit("simple:bit", 1, true)

	vb, _ := rd.Getbit("simple:bit", 0).Bool()
	c.Check(vb, Equals, true)
	vb, _ = rd.Getbit("simple:bit", 1).Bool()
	c.Check(vb, Equals, true)

	c.Check(rd.Get("non:existing:key").Type, Equals, ReplyNil)
	vb, _ = rd.Exists("non:existing:key").Bool()
	c.Check(vb, Equals, false)
	vb, _ = rd.Setnx("simple:nx", "Test").Bool()
	c.Check(vb, Equals, true)
	vb, _ = rd.Setnx("simple:nx", "Test").Bool()
	c.Check(vb, Equals, false)
}

// Test calls that return multiple values.
func (s *S) TestMultiple(c *C) {
	// Set values first.
	rd.Set("multiple:a", "a")
	rd.Set("multiple:b", "b")
	rd.Set("multiple:c", "c")

	mulstr, err := rd.Mget("multiple:a", "multiple:b", "multiple:c").List()
	c.Assert(err, IsNil)
	c.Check(
		mulstr,
		DeepEquals,
		[]string{"a", "b", "c"},
	)
}

// Test list calls.
func (s *S) TestList(c *C) {
	rd.Rpush("list:a", "one")
	rd.Rpush("list:a", "two")
	rd.Rpush("list:a", "three")
	rd.Rpush("list:a", "four")
	rd.Rpush("list:a", "five")
	rd.Rpush("list:a", "six")
	rd.Rpush("list:a", "seven")
	rd.Rpush("list:a", "eight")
	rd.Rpush("list:a", "nine")
	la, err := rd.Lrange("list:a", 0, -1).List()
	c.Assert(err, IsNil)
	c.Check(
		la,
		DeepEquals,
		[]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"})
	vs, _ := rd.Lpop("list:a").Str()
	c.Check(vs, Equals, "one")

	r := rd.Lrange("list:a", 3, 6)
	c.Assert(len(r.Elems), Equals, 4)
	vs, _ = r.Elems[0].Str()
	c.Check(vs, Equals, "five")
	vs, _ = r.Elems[1].Str()
	c.Check(vs, Equals, "six")
	vs, _ = r.Elems[2].Str()
	c.Check(vs, Equals, "seven")
	vs, _ = r.Elems[3].Str()
	c.Check(vs, Equals, "eight")

	rd.Ltrim("list:a", 0, 3)
	vi, _ := rd.Llen("list:a").Int()
	c.Check(vi, Equals, 4)

	rd.Rpoplpush("list:a", "list:b")
	c.Check(rd.Lindex("list:b", 4711).Type, Equals, ReplyNil)
	vs, _ = rd.Lindex("list:b", 0).Str()
	c.Check(vs, Equals, "five")

	rd.Rpush("list:c", 1)
	rd.Rpush("list:c", 2)
	rd.Rpush("list:c", 3)
	rd.Rpush("list:c", 4)
	rd.Rpush("list:c", 5)
	vs, _ = rd.Lpop("list:c").Str()
	c.Check(vs, Equals, "1")

	lnil, err := rd.Lrange("non-existent-list", 0, -1).List()
	c.Assert(err, IsNil)
	c.Check(lnil, DeepEquals, []string{})

	// nil elements
	rd.Rpush("list:d", "one")
	rd.Rpush("list:d", nil)
	rd.Rpush("list:d", "three")
	ld, err := rd.Lrange("list:d", 0, -1).List()
	c.Assert(err, IsNil)
	c.Check(
		ld,
		DeepEquals,
		[]string{"one", "", "three"})
}

// Test set calls.
func (s *S) TestSets(c *C) {
	rd.Sadd("set:a", 1)
	rd.Sadd("set:a", 2)
	rd.Sadd("set:a", 3)
	rd.Sadd("set:a", 4)
	rd.Sadd("set:a", 5)
	rd.Sadd("set:a", 4)
	rd.Sadd("set:a", 3)
	vi, _ := rd.Scard("set:a").Int()
	c.Check(vi, Equals, 5)
	vb, _ := rd.Sismember("set:a", "4").Bool()
	c.Check(vb, Equals, true)
}

// Test Reply.
func (s *S) TestReply(c *C) {
	// string
	rd.Set("foo", "bar")
	vs, _ := rd.Get("foo").Str()
	c.Check(
		vs,
		Equals,
		"bar")

	// []byte
	rd.Set("foo2", []byte{'b', 'a', 'r'})
	vbs, _ := rd.Get("foo2").Bytes()
	c.Check(
		vbs,
		DeepEquals,
		[]byte{'b', 'a', 'r'})

	// bool
	rd.Set("foo3", true)
	vb, _ := rd.Get("foo3").Bool()
	c.Check(
		vb,
		Equals,
		true)

	// integers
	rd.Set("foo4", 2)
	vs, _ = rd.Get("foo4").Str()
	c.Check(
		vs,
		Equals,
		"2")

	rd.Set("foo5", 2)
	vi, _ := rd.Get("foo5").Int()
	c.Check(
		vi,
		Equals,
		2)

	rd.Set("foo6", "2")
	vi, _ = rd.Get("foo6").Int()
	c.Check(
		vi,
		Equals,
		2)

	// list
	rd.Rpush("foo7", []int{1, 2, 3})
	foo7strings, err := rd.Lrange("foo7", 0, -1).List()
	c.Assert(err, IsNil)
	c.Check(
		foo7strings,
		DeepEquals,
		[]string{"1", "2", "3"})

	// hash
	rd.Hset("foo8", "k1", "v1")
	rd.Hset("foo8", "k2", nil)
	rd.Hset("foo8", "k3", "v3")

	foo8map, err := rd.Hgetall("foo8").Hash()
	c.Assert(err, IsNil)
	c.Check(
		foo8map,
		DeepEquals,
		map[string]string{
			"k1": "v1",
			"k2": "",
			"k3": "v3",
		})
}

// Test asynchronous calls.
func (s *S) TestAsync(c *C) {
	fut := rd.AsyncPing()
	r := fut.Reply()
	vs, _ := r.Str()
	c.Check(vs, Equals, "PONG")
}

// Test multi-value calls.
func (s *S) TestMulti(c *C) {
	rd.Sadd("multi:set", "one")
	rd.Sadd("multi:set", "two")
	rd.Sadd("multi:set", "three")

	c.Check(len(rd.Smembers("multi:set").Elems), Equals, 3)
}

// Test multicalls.
func (s *S) TestMultiCall(c *C) {
	r := rd.MultiCall(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Get("foo")
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	c.Check(r.Elems[0].Err, IsNil)
	vs, _ := r.Elems[1].Str()
	c.Check(vs, Equals, "bar")

	r = rd.MultiCall(func(mc *MultiCall) {
		mc.Set("foo2", "baz")
		mc.Get("foo2")
		rmc := mc.Flush()
		c.Check(rmc.Elems[0].Err, IsNil)
		vs, _ = rmc.Elems[1].Str()
		c.Check(vs, Equals, "baz")
		mc.Set("foo2", "qux")
		mc.Get("foo2")
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	c.Check(r.Elems[0].Err, IsNil)
	c.Check(r.Elems[1].Err, IsNil)
	vs, _ = r.Elems[1].Str()
	c.Check(vs, Equals, "qux")
}

// Test simple transactions.
func (s *S) TestTransaction(c *C) {
	r := rd.Transaction(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Get("foo")
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	vs, _ := r.Elems[0].Str()
	c.Check(vs, Equals, "OK")
	vs, _ = r.Elems[1].Str()
	c.Check(vs, Equals, "bar")

	// Flushing transaction
	r = rd.Transaction(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Flush()
		mc.Get("foo")
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	vs, _ = r.Elems[0].Str()
	c.Check(vs, Equals, "OK")
	vs, _ = r.Elems[1].Str()
	c.Check(vs, Equals, "bar")
}

// Test succesful complex tranactions.
func (s *S) TestComplexTransaction(c *C) {
	// Succesful transaction.
	r := rd.MultiCall(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Watch("foo")
		rmc := mc.Flush()
		c.Assert(rmc.Type, Equals, ReplyMulti)
		c.Assert(rmc.Elems[0].Err, IsNil)
		c.Assert(rmc.Elems[1].Err, IsNil)

		mc.Multi()
		mc.Set("foo", "baz")
		mc.Get("foo")
		mc.Call("brokenfunc")
		mc.Exec()
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	c.Check(r.Elems[0].Err, IsNil)
	c.Check(r.Elems[1].Err, IsNil)
	c.Check(r.Elems[2].Err, IsNil)
	c.Check(r.Elems[3].Err, NotNil)
	c.Check(r.Elems[4].Type, Equals, ReplyMulti)
	c.Check(len(r.Elems[4].Elems), Equals, 2)
	c.Check(r.Elems[4].Elems[0].Err, IsNil)
	vs, _ := r.Elems[4].Elems[1].Str()
	c.Check(vs, Equals, "baz")

	// Discarding transaction
	r = rd.MultiCall(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Multi()
		mc.Set("foo", "baz")
		mc.Discard()
		mc.Get("foo")
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	c.Check(r.Elems[0].Err, IsNil)
	c.Check(r.Elems[1].Err, IsNil)
	c.Check(r.Elems[2].Err, IsNil)
	c.Check(r.Elems[3].Err, IsNil)
	c.Check(r.Elems[4].Err, IsNil)
	vs, _ = r.Elems[4].Str()
	c.Check(vs, Equals, "bar")
}

// Test asynchronous multicalls.
func (s *S) TestAsyncMultiCall(c *C) {
	r := rd.AsyncMultiCall(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Get("foo")
	}).Reply()
	c.Assert(r.Type, Equals, ReplyMulti)
	vs, _ := r.Elems[1].Str()
	c.Check(vs, Equals, "bar")
}

// Test simple asynchronous transactions.
func (s *S) TestAsyncTransaction(c *C) {
	r := rd.AsyncTransaction(func(mc *MultiCall) {
		mc.Set("foo", "bar")
		mc.Get("foo")
	}).Reply()
	c.Assert(r.Type, Equals, ReplyMulti)
	vs, _ := r.Elems[0].Str()
	c.Check(vs, Equals, "OK")
	vs, _ = r.Elems[1].Str()
	c.Check(vs, Equals, "bar")
}

// Test Error.
func (s *S) TestError(c *C) {
	err := newError("foo", ErrorConnection)
	c.Check(err.Error(), Equals, "foo")
	c.Check(err.Test(ErrorConnection), Equals, true)
	c.Check(err.Test(ErrorRedis), Equals, false)

	errext := newErrorExt("bar", err, ErrorLoading)
	c.Check(errext.Error(), Equals, "bar: foo")
	c.Check(errext.Test(ErrorConnection), Equals, true)
	c.Check(errext.Test(ErrorLoading), Equals, true)
}

// Test tcp/ip connections.
func (s *S) TestTCP(c *C) {
	rdA := NewClient(conf)
	rep := rdA.Echo("Hello, World!")
	c.Assert(rep.Err, IsNil)
	vs, _ := rep.Str()
	c.Check(vs, Equals, "Hello, World!")
}

// Test unix connections.
func (s *S) TestUnix(c *C) {
	conf2 := DefaultConfig()
	conf2.Network = "unix"
	conf2.Address = "/tmp/redis.sock"
	rdA := NewClient(conf2)
	rep := rdA.Echo("Hello, World!")
	vs, err := rep.Str()
	c.Assert(err, IsNil)
	c.Check(vs, Equals, "Hello, World!")
}

// Test Client.InfoMap.
func (s *S) TestInfoMap(c *C) {
	im, err := rd.InfoMap()
	c.Assert(err, IsNil)
	c.Check(im["pubsub_patterns"], Equals, "0")
}

//* Long tests

// Test Subscription.
func (s *Long) TestSubscription(c *C) {
	var messages []*Message
	msgHdlr := func(msg *Message) {
		c.Log(msg)
		messages = append(messages, msg)
	}

	sub, err := rd.Subscription(msgHdlr)
	if err != nil {
		c.Errorf("Failed to subscribe: '%v'!", err)
		return
	}
	defer sub.Close()

	sub.Subscribe("chan1", "chan2")
	time.Sleep(time.Second)

	vi, _ := rd.Publish("chan1", "foo").Int()
	c.Check(vi, Equals, 1)
	time.Sleep(time.Second)

	sub.Unsubscribe("chan1")
	time.Sleep(time.Second)

	vi, _ = rd.Publish("chan1", "bar").Int()
	c.Check(vi, Equals, 0)
	time.Sleep(time.Second)

	c.Assert(len(messages), Equals, 4)
	c.Check(messages[0].Type, Equals, MessageSubscribe)
	c.Check(messages[0].Channel, Equals, "chan1")
	c.Check(messages[0].Subscriptions, Equals, 1)
	c.Check(messages[1].Type, Equals, MessageSubscribe)
	c.Check(messages[1].Channel, Equals, "chan2")
	c.Check(messages[1].Subscriptions, Equals, 2)
	c.Check(messages[2].Type, Equals, MessageMessage)
	c.Check(messages[2].Channel, Equals, "chan1")
	c.Check(messages[2].Payload, Equals, "foo")
	c.Check(messages[3].Type, Equals, MessageUnsubscribe)
	c.Check(messages[3].Channel, Equals, "chan1")
	c.Check(messages[3].Subscriptions, Equals, 1)
}

// Test pattern subscriptions.
func (s *Long) TestPsubscribe(c *C) {
	var messages []*Message
	msgHdlr := func(msg *Message) {
		c.Log(msg)
		messages = append(messages, msg)
	}

	sub, err := rd.Subscription(msgHdlr)
	if err != nil {
		c.Errorf("Failed to subscribe: '%v'!", err)
		return
	}
	defer sub.Close()

	sub.Psubscribe("foo.*")
	time.Sleep(time.Second)

	vi, _ := rd.Publish("foo.foo", "foo").Int()
	c.Check(vi, Equals, 1)
	time.Sleep(time.Second)

	sub.Punsubscribe("foo.*")
	time.Sleep(time.Second)

	vi, _ = rd.Publish("foo.bar", "bar").Int()
	c.Check(vi, Equals, 0)
	time.Sleep(time.Second)

	c.Assert(len(messages), Equals, 3)
	c.Check(messages[0].Type, Equals, MessagePsubscribe)
	c.Check(messages[0].Pattern, Equals, "foo.*")
	c.Check(messages[0].Subscriptions, Equals, 1)
	c.Check(messages[1].Type, Equals, MessagePmessage)
	c.Check(messages[1].Pattern, Equals, "foo.*")
	c.Check(messages[1].Channel, Equals, "foo.foo")
	c.Check(messages[1].Payload, Equals, "foo")
	c.Check(messages[2].Type, Equals, MessagePunsubscribe)
	c.Check(messages[2].Pattern, Equals, "foo.*")
	c.Check(messages[2].Subscriptions, Equals, 0)
}

// Test aborting complex tranactions.
func (s *Long) TestAbortingComplexTransaction(c *C) {
	go func() {
		time.Sleep(time.Second)
		rd.Set("foo", 9)
	}()

	r := rd.MultiCall(func(mc *MultiCall) {
		mc.Set("foo", 1)
		mc.Watch("foo")
		mc.Multi()
		rmc := mc.Flush()
		c.Assert(rmc.Type, Equals, ReplyMulti)
		c.Check(rmc.Elems[0].Err, IsNil)
		c.Check(rmc.Elems[1].Err, IsNil)
		c.Check(rmc.Elems[2].Err, IsNil)

		time.Sleep(time.Second * 2)
		mc.Set("foo", 2)
		mc.Exec()
	})
	c.Assert(r.Type, Equals, ReplyMulti)
	c.Check(r.Elems[1].Type, Equals, ReplyNil)
}

// Test illegal database.
func (s *Long) TestIllegalDatabase(c *C) {
	conf2 := conf
	conf2.Database = 4711
	rdA := NewClient(conf2)
	rA := rdA.Ping()
	c.Check(rA.Err, NotNil)
}

//* Utils tests

// Test formatArg().
func (s *Utils) TestFormatArg(c *C) {
	c.Check(formatArg("foo"), DeepEquals, []byte("$3\r\nfoo\r\n"))
	c.Check(formatArg("世界"), DeepEquals, []byte("$6\r\n\xe4\xb8\x96\xe7\x95\x8c\r\n"))
	c.Check(formatArg(int(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(int8(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(int16(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(int32(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(int64(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(uint(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(uint8(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(uint16(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(uint32(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(uint64(5)), DeepEquals, []byte("$1\r\n5\r\n"))
	c.Check(formatArg(true), DeepEquals, []byte("$1\r\n1\r\n"))
	c.Check(formatArg(false), DeepEquals, []byte("$1\r\n0\r\n"))
	c.Check(formatArg([]interface{}{"foo", 5, true}), DeepEquals,
		[]byte("$3\r\nfoo\r\n$1\r\n5\r\n$1\r\n1\r\n"))
	c.Check(formatArg(map[interface{}]interface{}{1: "foo"}), DeepEquals,
		[]byte("$1\r\n1\r\n$3\r\nfoo\r\n"))
	c.Check(formatArg(1.5), DeepEquals, []byte("$3\r\n1.5\r\n"))
}

// Test createRequest().
func (s *Utils) TestCreateRequest(c *C) {
	c.Check(createRequest(call{cmd: "PING"}), DeepEquals, []byte("*1\r\n$4\r\nPING\r\n"))
	c.Check(createRequest(call{
		cmd:  "SET",
		args: []interface{}{"key", 5},
	}),
		DeepEquals, []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1\r\n5\r\n"))
}

func BenchmarkCreateRequest(b *testing.B) {
	for i := 0; i < b.N; i++ {
		createRequest(call{
			cmd:  cmdSet,
			args: []interface{}{"foo", "bar"},
		})
	}
}
