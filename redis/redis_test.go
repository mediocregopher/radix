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

func setUpTest(c *C) {
	rd = NewClient(Configuration{
		Database: 8,
		Address:  "127.0.0.1:6379"})

	rs := rd.Command("flushall")
	if !rs.OK() {
		c.Fatalf("setUp FLUSHALL failed: %s", rs.Error())
	}
}

func tearDownTest(c *C) {
	rs := rd.Command("flushall")
	if !rs.OK() {
		c.Fatalf("tearDown FLUSHALL failed: %s", rs.Error())
	}
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
	rd.Command("set", "foo", "bar")

	rdA := NewClient(Configuration{Database: 9})
	c.Check(rdA.Command("get", "foo").String(), Equals, "bar")
}

// Test connection commands.
func (s *S) TestConnection(c *C) {
	c.Check(rd.Command("echo", "Hello, World!").String(), Equals, "Hello, World!")
	c.Check(rd.Command("ping").String(), Equals, "PONG")
}

// Test single return value commands.
func (s *S) TestSimpleValue(c *C) {
	// Simple value commands.
	rd.Command("set", "simple:string", "Hello,")
	rd.Command("append", "simple:string", " World!")
	c.Check(rd.Command("get", "simple:string").String(), Equals, "Hello, World!")

	rd.Command("set", "simple:int", 10)
	c.Check(rd.Command("incr", "simple:int").Int(), Equals, 11)

	rd.Command("set", "simple:float64", 47.11)
	c.Check(rd.Command("get", "simple:float64").Value().Float64(), Equals, 47.11)

	rd.Command("setbit", "simple:bit", 0, true)
	rd.Command("setbit", "simple:bit", 1, true)
	c.Check(rd.Command("getbit", "simple:bit", 0).Bool(), Equals, true)
	c.Check(rd.Command("getbit", "simple:bit", 1).Bool(), Equals, true)

	c.Check(rd.Command("get", "non:existing:key").OK(), Equals, false)
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

	c.Check(
		rd.Command("mget", "multiple:a", "multiple:b", "multiple:c").Strings(),
		Equals,
		[]string{"a", "b", "c"})
}

// Test hash accessing.
func (s *S) TestHash(c *C) {
	//* Single  return value commands.
	rd.Command("hset", "hash:bool", "true:1", 1)
	rd.Command("hset", "hash:bool", "true:2", true)
	rd.Command("hset", "hash:bool", "true:3", "T")
	rd.Command("hset", "hash:bool", "false:1", 0)
	rd.Command("hset", "hash:bool", "false:2", false)
	rd.Command("hset", "hash:bool", "false:3", "FALSE")
	c.Check(rd.Command("hget", "hash:bool", "true:1").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "true:2").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "true:3").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "false:1").Bool(), Equals, false)
	c.Check(rd.Command("hget", "hash:bool", "false:2").Bool(), Equals, false)
	c.Check(rd.Command("hget", "hash:bool", "false:3").Bool(), Equals, false)

	ha := rd.Command("hgetall", "hash:bool").Hash()
	c.Assert(len(ha), Equals, 6)
	c.Check(ha.Bool("true:1"), Equals, true)
	c.Check(ha.Bool("true:2"), Equals, true)
	c.Check(ha.Bool("true:3"), Equals, true)
	c.Check(ha.Bool("false:1"), Equals, false)
	c.Check(ha.Bool("false:2"), Equals, false)
	c.Check(ha.Bool("false:3"), Equals, false)
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
	c.Check(
		rd.Command("lrange", "list:a", 0, -1).Strings(),
		Equals,
		[]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"})
	c.Check(rd.Command("lpop", "list:a").String(), Equals, "one")

	vs := rd.Command("lrange", "list:a", 3, 6).Values()
	c.Assert(len(vs), Equals, 4)
	c.Check(vs[0].String(), Equals, "five")
	c.Check(vs[1].String(), Equals, "six")
	c.Check(vs[2].String(), Equals, "seven")
	c.Check(vs[3].String(), Equals, "eight")

	rd.Command("ltrim", "list:a", 0, 3)
	c.Check(rd.Command("llen", "list:a").Int(), Equals, 4)

	rd.Command("rpoplpush", "list:a", "list:b")
	c.Check(rd.Command("lindex", "list:b", 4711).OK(), Equals, false)
	c.Check(rd.Command("lindex", "list:b", 0).String(), Equals, "five")

	rd.Command("rpush", "list:c", 1)
	rd.Command("rpush", "list:c", 2)
	rd.Command("rpush", "list:c", 3)
	rd.Command("rpush", "list:c", 4)
	rd.Command("rpush", "list:c", 5)
	c.Check(rd.Command("lpop", "list:c").Int(), Equals, 1)
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
		rd.Command("get", "foo").String(),
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
		rd.Command("get", "foo3").Bytes(),
		Equals,
		[]byte{'1'})

	// integers
	rd.Command("set", "foo4", 2)
	c.Check(
		rd.Command("get", "foo4").Int(),
		Equals,
		2)

	// slice
	rd.Command("rpush", "foo5", []int{1, 2, 3})
	c.Check(
		rd.Command("lrange", "foo5", 0, -1).Ints(),
		Equals,
		[]int{1, 2, 3})
}

// Test asynchronous commands.
func (s *S) TestAsync(c *C) {
	fut := rd.AsyncCommand("PING")
	rs := fut.ResultSet()
	c.Check(rs.String(), Equals, "PONG")
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
	rs := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	})
	c.Check(rs.ResultSetAt(0).OK(), Equals, true)
	c.Check(rs.ResultSetAt(1).String(), Equals, "bar")

	rs = rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo2", "baz")
		mc.Command("get", "foo2")
		rsmc := mc.Flush()
		c.Check(rsmc.ResultSetAt(0).OK(), Equals, true)
		c.Check(rsmc.ResultSetAt(1).String(), Equals, "baz")
		mc.Command("set", "foo2", "qux")
		mc.Command("get", "foo2")
	})
	c.Check(rs.ResultSetAt(0).OK(), Equals, true)
	c.Check(rs.ResultSetAt(1).String(), Equals, "qux")
}

// Test subscribe.
func (s *S) TestSubscribe(c *C) {
	sub, numSubs, err := rd.Subscribe("subscribe:one", "subscribe:two")
	if err != nil {
		c.Errorf("Can't subscribe: '%v'!", err)
		return
	}
	c.Check(numSubs, Equals, 2)

	go func() {
		for sv := range sub.SubscriptionValueChan {
			if sv == nil {
				c.Log("Received nil!")
			} else {
				c.Logf("Published '%v' Channel '%v' Pattern '%v'", sv, sv.Channel, sv.ChannelPattern)
			}
		}
		c.Log("Subscription stopped!")
	}()
	c.Check(rd.Publish("subscribe:one", "1 Alpha"), Equals, 1)

	rd.Publish("subscribe:one", "1 Beta")
	rd.Publish("subscribe:one", "1 Gamma")
	rd.Publish("subscribe:two", "2 Alpha")
	rd.Publish("subscribe:two", "2 Beta")
	c.Log(sub.Unsubscribe("subscribe:two"))
	c.Log(sub.Unsubscribe("subscribe:one"))
	c.Check(rd.Publish("subscribe:two", "2 Gamma"), Equals, 0)

	sub.Subscribe("subscribe:*")
	rd.Publish("subscribe:one", "Pattern 1")
	rd.Publish("subscribe:two", "Pattern 2")
	sub.Stop()
}

//* Long tests

// Test pop.
func (s *Long) TestPop(c *C) {
	fooPush := func(rd *Client) {
		time.Sleep(time.Second)
		rd.Command("lpush", "pop:first", "foo")
	}

	// Set A: no database timeout.
	rdA := NewClient(Configuration{})

	go fooPush(rdA)

	rsAA := rdA.Command("blpop", "pop:first", 5)
	kv := rsAA.KeyValue()
	c.Check(kv.Value.String(), Equals, "foo")

	rsAB := rdA.Command("blpop", "pop:first", 1)
	c.Check(rsAB.OK(), Equals, true)

	// Set B: database with timeout.
	rdB := NewClient(Configuration{})

	rsBA := rdB.Command("blpop", "pop:first", 1)
	c.Check(rsBA.OK(), Equals, true)
}

// Test illegal databases.
func (s *Long) TestIllegalDatabases(c *C) {
	c.Log("Test selecting an illegal database...")
	rdA := NewClient(Configuration{Database: 4711})
	rsA := rdA.Command("ping")
	c.Check(rsA.OK(), Equals, true)

	c.Log("Test connecting to an illegal address...")
	rdB := NewClient(Configuration{Address: "192.168.100.100:12345"})
	rsB := rdB.Command("ping")
	c.Check(rsB.OK(), Equals, true)
}

// Test database killing with a long run.
func (s *Long) TestDatabaseKill(c *C) {
	rdA := NewClient(Configuration{PoolSize: 5})

	for i := 1; i < 120; i++ {
		if !rdA.Command("set", "long:run", i).OK() {
			c.Errorf("Long run failed!")
			return
		}
		time.Sleep(time.Second)
	}
}
