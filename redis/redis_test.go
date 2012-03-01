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
var conf Configuration = Configuration{
	Database: 8,
	Address:  "127.0.0.1:6379",
	Timeout:  10,
}

type TI interface {
	Fatalf(string, ...interface{})
}

func setUpTest(c TI) {
	rd = NewClient(conf)

	r := rd.Command("flushall")
	if r.Error() != nil {
		c.Fatalf("setUp FLUSHALL failed: %s", r.Error())
	}
}

func tearDownTest(c TI) {
	r := rd.Command("flushall")
	if r.Error() != nil {
		c.Fatalf("tearDown FLUSHALL failed: %s", r.Error())
	}
	rd.Close()
}

//* Tests
type S struct{}
type Long struct{}

var long = flag.Bool("long", false, "Include blocking tests")

func init() {
	Suite(&S{})
	Suite(&Long{})
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

// Test Select.
func (s *S) TestSelect(c *C) {
	rd.Select(9)

	c.Check(rd.configuration.Database, Equals, 9)
	rep := rd.Command("set", "foo", "bar")
	c.Assert(rep.Error(), IsNil)

	conf2 := conf
	conf2.Database = 9
	rdA := NewClient(conf2)
	rep = rdA.Command("get", "foo")
	if rep.Error() != nil {
		c.Log(rep.Error())
	}
	c.Check(rdA.Command("get", "foo").Str(), Equals, "bar")
}

// Test connection commands.
func (s *S) TestConnection(c *C) {
	c.Check(rd.Command("echo", "Hello, World!").Str(), Equals, "Hello, World!")
	c.Check(rd.Command("ping").Str(), Equals, "PONG")
}

// Test single return value commands.
func (s *S) TestSimpleValue(c *C) {
	// Simple value commands.
	rd.Command("set", "simple:string", "Hello,")
	rd.Command("append", "simple:string", " World!")
	c.Check(rd.Command("get", "simple:string").Str(), Equals, "Hello, World!")

	rd.Command("set", "simple:int", 10)
	c.Check(rd.Command("incr", "simple:int").Int(), Equals, 11)

	rd.Command("setbit", "simple:bit", 0, true)
	rd.Command("setbit", "simple:bit", 1, true)
	c.Check(rd.Command("getbit", "simple:bit", 0).Bool(), Equals, true)
	c.Check(rd.Command("getbit", "simple:bit", 1).Bool(), Equals, true)

	c.Check(rd.Command("get", "non:existing:key").Nil(), Equals, true)
	c.Check(rd.Command("exists", "non:existing:key").Bool(), Equals, false)
	c.Check(rd.Command("setnx", "simple:nx", "Test").Bool(), Equals, true)
	c.Check(rd.Command("setnx", "simple:nx", "Test").Bool(), Equals, false)
}

// Test multi return value commands.
func (s *S) TestMultiple(c *C) {
	// Set values first.
	rd.Command("set", "multiple:a", "a")
	rd.Command("set", "multiple:b", "b")
	rd.Command("set", "multiple:c", "c")

	mulstr, err := rd.Command("mget", "multiple:a", "multiple:b", "multiple:c").Strings()
	c.Assert(err, IsNil)
	c.Check(
		mulstr,
		Equals,
		[]string{"a", "b", "c"})
}

// Test hash accessing.
func (s *S) TestHash(c *C) {
	//* Single  return value commands.
	rd.Command("hset", "hash:bool", "true:1", 1)
	rd.Command("hset", "hash:bool", "true:2", true)
	rd.Command("hset", "hash:bool", "true:3", "1")
	rd.Command("hset", "hash:bool", "false:1", 0)
	rd.Command("hset", "hash:bool", "false:2", false)
	rd.Command("hset", "hash:bool", "false:3", "0")
	c.Check(rd.Command("hget", "hash:bool", "true:1").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "true:2").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "true:3").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "false:1").Bool(), Equals, false)
	c.Check(rd.Command("hget", "hash:bool", "false:2").Bool(), Equals, false)
	c.Check(rd.Command("hget", "hash:bool", "false:3").Bool(), Equals, false)

	ha, err := rd.Command("hgetall", "hash:bool").Map()
	c.Assert(err, IsNil)
	c.Check(ha["true:1"].Bool(), Equals, true)
	c.Check(ha["true:2"].Bool(), Equals, true)
	c.Check(ha["true:3"].Bool(), Equals, true)
	c.Check(ha["false:1"].Bool(), Equals, false)
	c.Check(ha["false:2"].Bool(), Equals, false)
	c.Check(ha["false:3"].Bool(), Equals, false)
}

// Test list commands.
func (s *S) TestList(c *C) {
	rd.Command("rpush", "list:a", "one")
	rd.Command("rpush", "list:a", "two")
	rd.Command("rpush", "list:a", "three")
	rd.Command("rpush", "list:a", "four")
	rd.Command("rpush", "list:a", "five")
	rd.Command("rpush", "list:a", "six")
	rd.Command("rpush", "list:a", "seven")
	rd.Command("rpush", "list:a", "eight")
	rd.Command("rpush", "list:a", "nine")
	lranges, err := rd.Command("lrange", "list:a", 0, -1).Strings()
	c.Assert(err, IsNil)
	c.Check(
		lranges,
		Equals,
		[]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"})
	c.Check(rd.Command("lpop", "list:a").Str(), Equals, "one")

	elems := rd.Command("lrange", "list:a", 3, 6).Elems()
	c.Assert(len(elems), Equals, 4)
	c.Check(elems[0].Str(), Equals, "five")
	c.Check(elems[1].Str(), Equals, "six")
	c.Check(elems[2].Str(), Equals, "seven")
	c.Check(elems[3].Str(), Equals, "eight")

	rd.Command("ltrim", "list:a", 0, 3)
	c.Check(rd.Command("llen", "list:a").Int(), Equals, 4)

	rd.Command("rpoplpush", "list:a", "list:b")
	c.Check(rd.Command("lindex", "list:b", 4711).Nil(), Equals, true)
	c.Check(rd.Command("lindex", "list:b", 0).Str(), Equals, "five")

	rd.Command("rpush", "list:c", 1)
	rd.Command("rpush", "list:c", 2)
	rd.Command("rpush", "list:c", 3)
	rd.Command("rpush", "list:c", 4)
	rd.Command("rpush", "list:c", 5)
	c.Check(rd.Command("lpop", "list:c").Str(), Equals, "1")

	lrangenil, err := rd.Command("lrange", "non-existent-list", 0, -1).Strings()
	c.Assert(err, IsNil)
	c.Check(lrangenil, Equals, []string{})
}

// Test set commands.
func (s *S) TestSets(c *C) {
	rd.Command("sadd", "set:a", 1)
	rd.Command("sadd", "set:a", 2)
	rd.Command("sadd", "set:a", 3)
	rd.Command("sadd", "set:a", 4)
	rd.Command("sadd", "set:a", 5)
	rd.Command("sadd", "set:a", 4)
	rd.Command("sadd", "set:a", 3)
	c.Check(rd.Command("scard", "set:a").Int(), Equals, 5)
	c.Check(rd.Command("sismember", "set:a", "4").Bool(), Equals, true)
}

// Test argument formatting.
func (s *S) TestArgToRedis(c *C) {
	// string
	rd.Command("set", "foo", "bar")
	c.Check(
		rd.Command("get", "foo").Str(),
		Equals,
		"bar")

	// []byte
	rd.Command("set", "foo2", []byte{'b', 'a', 'r'})
	c.Check(
		rd.Command("get", "foo2").Bytes(),
		Equals,
		[]byte{'b', 'a', 'r'})

	// bool
	rd.Command("set", "foo3", true)
	c.Check(
		rd.Command("get", "foo3").Bool(),
		Equals,
		true)

	// integers
	rd.Command("set", "foo4", 2)
	c.Check(
		rd.Command("get", "foo4").Str(),
		Equals,
		"2")

	// slice
	rd.Command("rpush", "foo5", []int{1, 2, 3})
	foo5strings, err := rd.Command("lrange", "foo5", 0, -1).Strings()
	c.Assert(err, IsNil)
	c.Check(
		foo5strings,
		Equals,
		[]string{"1", "2", "3"})

	// map
	rd.Command("hset", "foo6", "k1", "v1")
	rd.Command("hset", "foo6", "k2", "v2")
	rd.Command("hset", "foo6", "k3", "v3")

	foo6map, err := rd.Command("hgetall", "foo6").StringMap()
	c.Assert(err, IsNil)
	c.Check(
		foo6map,
		Equals,
		map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		})
}

// Test asynchronous commands.
func (s *S) TestAsync(c *C) {
	fut := rd.AsyncCommand("PING")
	r := fut.Reply()
	c.Check(r.Str(), Equals, "PONG")
}

// Test multi-value commands.
func (s *S) TestMulti(c *C) {
	rd.Command("sadd", "multi:set", "one")
	rd.Command("sadd", "multi:set", "two")
	rd.Command("sadd", "multi:set", "three")

	c.Check(rd.Command("smembers", "multi:set").Len(), Equals, 3)
}

// Test multi commands.
func (s *S) TestMultiCommand(c *C) {
	r := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Error(), IsNil)
	c.Check(r.At(1).Str(), Equals, "bar")

	r = rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo2", "baz")
		mc.Command("get", "foo2")
		rmc := mc.Flush()
		c.Check(rmc.At(0).Error(), IsNil)
		c.Check(rmc.At(1).Str(), Equals, "baz")
		mc.Command("set", "foo2", "qux")
		mc.Command("get", "foo2")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Error(), IsNil)
	c.Check(r.At(1).Str(), Equals, "qux")
}

// Test simple transactions.
func (s *S) TestTransaction(c *C) {
	r := rd.Transaction(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Str(), Equals, "OK")
	c.Check(r.At(1).Str(), Equals, "bar")

	// Flushing transaction
	r = rd.Transaction(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Flush()
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.Len(), Equals, 2)
	c.Check(r.At(0).Str(), Equals, "OK")
	c.Check(r.At(1).Str(), Equals, "bar")
}

// Test succesful complex tranactions.
func (s *S) TestComplexTransaction(c *C) {
	// Succesful transaction.
	r := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("watch", "foo")
		rmc := mc.Flush()
		c.Assert(rmc.Type(), Equals, ReplyMulti)
		c.Assert(rmc.Len(), Equals, 2)
		c.Assert(rmc.At(0).Error(), IsNil)
		c.Assert(rmc.At(1).Error(), IsNil)

		mc.Command("multi")
		mc.Command("set", "foo", "baz")
		mc.Command("get", "foo")
		mc.Command("brokenfunc")
		mc.Command("exec")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Assert(r.Len(), Equals, 5)
	c.Check(r.At(0).Error(), IsNil)
	c.Check(r.At(1).Error(), IsNil)
	c.Check(r.At(2).Error(), IsNil)
	c.Check(r.At(3).Error(), NotNil)
	c.Assert(r.At(4).Type(), Equals, ReplyMulti)
	c.Assert(r.At(4).Len(), Equals, 2)
	c.Check(r.At(4).At(0).Error(), IsNil)
	c.Check(r.At(4).At(1).Str(), Equals, "baz")

	// Discarding transaction
	r = rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("multi")
		mc.Command("set", "foo", "baz")
		mc.Command("discard")
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Assert(r.Len(), Equals, 5)
	c.Check(r.At(0).Error(), IsNil)
	c.Check(r.At(1).Error(), IsNil)
	c.Check(r.At(2).Error(), IsNil)
	c.Check(r.At(3).Error(), IsNil)
	c.Check(r.At(4).Error(), IsNil)
	c.Check(r.At(4).Str(), Equals, "bar")
}

// Test asynchronous multi commands.
func (s *S) TestAsyncMultiCommand(c *C) {
	r := rd.AsyncMultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	}).Reply()
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Error(), IsNil)
	c.Check(r.At(1).Str(), Equals, "bar")
}

// Test simple asynchronous transactions.
func (s *S) TestAsyncTransaction(c *C) {
	r := rd.AsyncTransaction(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	}).Reply()
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Str(), Equals, "OK")
	c.Check(r.At(1).Str(), Equals, "bar")
}

// Test Subscription.
func (s *S) TestSubscription(c *C) {
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

	sub.Subscribe("chan1", "chan2")

	c.Check(rd.Command("publish", "chan1", "foo").Int(), Equals, 1)
	sub.Unsubscribe("chan1")
	c.Check(rd.Command("publish", "chan1", "bar").Int(), Equals, 0)
	sub.Close()

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
}

// Test pattern subscriptions.
func (s *S) TestPSubscribe(c *C) {
	var messages []*Message
	msgHdlr := func(msg *Message) {
		//c.Log(msg)
		messages = append(messages, msg)
	}

	sub, err := rd.Subscription(msgHdlr)
	if err != nil {
		c.Errorf("Failed to subscribe: '%v'!", err)
		return
	}

	sub.PSubscribe("foo.*")

	c.Check(rd.Command("publish", "foo.foo", "foo").Int(), Equals, 1)
	sub.PUnsubscribe("foo.*")
	c.Check(rd.Command("publish", "foo.bar", "bar").Int(), Equals, 0)
	sub.Close()

	c.Assert(len(messages), Equals, 3)
	c.Check(messages[0].Type, Equals, MessagePSubscribe)
	c.Check(messages[0].Pattern, Equals, "foo.*")
	c.Check(messages[0].Subscriptions, Equals, 1)
	c.Check(messages[1].Type, Equals, MessagePMessage)
	c.Check(messages[1].Channel, Equals, "foo.foo")
	c.Check(messages[1].Payload, Equals, "foo")
	c.Check(messages[1].Pattern, Equals, "foo.*")
	c.Check(messages[2].Type, Equals, MessagePUnsubscribe)
	c.Check(messages[2].Pattern, Equals, "foo.*")
}

// Test errors.
func (s *S) TestError(c *C) {
	err := newError("foo", ErrorConnection)
	c.Check(err.Error(), Equals, "redis: foo")
	c.Check(err.Test(ErrorConnection), Equals, true)
	c.Check(err.Test(ErrorRedis), Equals, false)

	errext := newErrorExt("bar", err, ErrorLoading)
	c.Check(errext.Error(), Equals, "redis: bar")
	c.Check(errext.Test(ErrorConnection), Equals, true)
	c.Check(errext.Test(ErrorLoading), Equals, true)
}

// Test unix connections.
func (s *S) TestUnix(c *C) {
	conf2 := conf
	conf2.Address = ""
	conf2.Path = "/tmp/redis.sock"
	rdA := NewClient(conf2)
	rep := rdA.Command("echo", "Hello, World!")
	c.Assert(rep.Error(), IsNil)
	c.Check(rep.Str(), Equals, "Hello, World!")
}

//* Long tests

// Test aborting complex tranactions.
func (s *Long) TestAbortingComplexTransaction(c *C) {
	go func() {
		time.Sleep(time.Second)
		rd.Command("set", "foo", 9)
	}()

	r := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", 1)
		mc.Command("watch", "foo")
		mc.Command("multi")
		rmc := mc.Flush()
		c.Assert(rmc.Type(), Equals, ReplyMulti)
		c.Assert(rmc.Len(), Equals, 3)
		c.Assert(rmc.At(0).Error(), IsNil)
		c.Assert(rmc.At(1).Error(), IsNil)
		c.Assert(rmc.At(2).Error(), IsNil)

		time.Sleep(time.Second * 2)
		mc.Command("set", "foo", 2)
		mc.Command("exec")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Assert(r.Len(), Equals, 2)
	c.Check(r.At(1).Nil(), Equals, true)
}

// Test illegal databases.
func (s *Long) TestIllegalDatabases(c *C) {
	conf2 := conf
	conf2.Database = 4711
	rdA := NewClient(conf2)
	rA := rdA.Command("ping")
	c.Check(rA.Error(), NotNil)

	conf3 := conf
	conf3.Address = "192.168.100.100:12345"
	rdB := NewClient(conf3)
	rB := rdB.Command("ping")
	c.Check(rB.Error(), NotNil)
}

//* Benchmarks

func BenchmarkBlockingPing(b *testing.B) {
	setUpTest(b)

	for i := 0; i < b.N; i++ {
		rd.Command("ping")
	}

	tearDownTest(b)
}

func BenchmarkBlockingSet(b *testing.B) {
	setUpTest(b)

	for i := 0; i < b.N; i++ {
		rd.Command("set", "foo", "bar")
	}

	tearDownTest(b)
}

func BenchmarkBlockingGet(b *testing.B) {
	setUpTest(b)

	for i := 0; i < b.N; i++ {
		rd.Command("get", "foo", "bar")
	}

	tearDownTest(b)
}

func BenchmarkAsyncPing(b *testing.B) {
	setUpTest(b)

	for i := 0; i < b.N; i++ {
		fut := rd.AsyncCommand("ping")
		fut.Reply()
	}

	tearDownTest(b)
}

func BenchmarkAsyncSet(b *testing.B) {
	setUpTest(b)

	for i := 0; i < b.N; i++ {
		fut := rd.AsyncCommand("set", "foo", "bar")
		fut.Reply()
	}

	tearDownTest(b)
}

func BenchmarkAsyncGet(b *testing.B) {
	setUpTest(b)

	for i := 0; i < b.N; i++ {
		fut := rd.AsyncCommand("get", "foo", "bar")
		fut.Reply()
	}

	tearDownTest(b)
}
