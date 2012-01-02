package redis

import (
	. "launchpad.net/gocheck"
	"testing"
	"time"
	"flag"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

//* Helpers
var rd *RedisDatabase

// hashableTestType is a simple type implementing the
// Hashable interface.
type hashableTestType struct {
	a string
	b int64
	c bool
	d float64
}

// GetHash returns the fields as hash.
func (htt *hashableTestType) GetHash() Hash {
	h := NewHash()

	h.Set("hashable:field:a", htt.a)
	h.Set("hashable:field:b", htt.b)
	h.Set("hashable:field:c", htt.c)
	h.Set("hashable:field:d", htt.d)

	return h
}

// SetHash sets the fields from a hash.
func (htt *hashableTestType) SetHash(h Hash) {
	htt.a = h.String("hashable:field:a")
	htt.b = h.Int64("hashable:field:b")
	htt.c = h.Bool("hashable:field:c")
	htt.d = h.Float64("hashable:field:d")
}

func setUpTest(c *C) {
	rd = NewRedisDatabase(Configuration{
		Database: 8,
		Address:  "127.0.0.1:6379"})

	// We use databases 8 and 9 for testing.
	rs := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("flushdb") // flush db 8
		mc.Command("select", "9")
		mc.Command("flushdb")
		mc.Command("select", "8")
	})

	if !rs.OK() {
		c.Fatal("Setting up test databases failed.")
	}
}

func tearDownTest(c *C) {
	// Clean up our changes.
	rs := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("select", "8")
		mc.Command("flushdb")
		mc.Command("select", "9")
		mc.Command("flushdb")
	})

	if !rs.OK() {
		c.Fatal("Cleaning up test databases failed.")
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

	rd.Command("mset", hashableTestType{"multi", 4711, true, 3.141})
	if v := rd.Command("mget", "hashable:field:a", "hashable:field:c").Values(); len(v) != 2 {
	}
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
	c.Assert(ha.Len(), Equals, 6)
	c.Check(ha.Bool("true:1"), Equals, true)
	c.Check(ha.Bool("true:2"), Equals, true)
	c.Check(ha.Bool("true:3"), Equals, true)
	c.Check(ha.Bool("false:1"), Equals, false)
	c.Check(ha.Bool("false:2"), Equals, false)
	c.Check(ha.Bool("false:3"), Equals, false)

	hb := hashableTestType{`foo "bar" yadda`, 4711, true, 8.15}
	rd.Command("hmset", "hashable", hb.GetHash())
	rd.Command("hincrby", "hashable", "hashable:field:b", 289)
	hb = hashableTestType{}
	hb.SetHash(rd.Command("hgetall", "hashable").Hash())
	c.Check(hb.a, Equals, `foo "bar" yadda`)
	c.Check(hb.b, Equals, int64(5000))
	c.Check(hb.c, Equals, true)
	c.Check(hb.d, Equals, 8.15)
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
	c.Check(rd.Command("llen", "list:a").Int(), Equals, 9)
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
func (s *S) TestSet(c *C) {
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

// Test asynchronous commands.
func (s *S) TestAsync(c *C) {
	fut := rd.AsyncCommand("PING")
    rs := fut.ResultSet()
	c.Check(rs.String(), Equals, "PONG")
}

// Test complex commands.
func (s *S) TestComplex(c *C) {
	rsA := rd.Command("info")
	c.Check(rsA.Value().StringMap()["arch_bits"], NotNil)

	sliceIn := []string{"A", "B", "C", "D", "E"}
	rd.Command("set", "complex:slice", sliceIn)
	rsB := rd.Command("get", "complex:slice")
	sliceOut := rsB.Value().StringSlice()

	for i, s := range sliceOut {
		if sliceIn[i] != s {
			c.Errorf("Got '%v', expected '%v'!", s, sliceIn[i])
		}
	}

	mapIn := map[string]string{
		"A": "1",
		"B": "2",
		"C": "3",
		"D": "4",
		"E": "5",
	}

	rd.Command("set", "complex:map", mapIn)
	rsC := rd.Command("get", "complex:map")
	mapOut := rsC.Value().StringMap()

	for k, v := range mapOut {
		if mapIn[k] != v {
			c.Errorf("Got '%v', expected '%v'!", v, mapIn[k])
		}
	}
}

// Test multi-value commands.
func (s *S) TestMulti(c *C) {
	rd.Command("sadd", "multi:set", "one")
	rd.Command("sadd", "multi:set", "two")
	rd.Command("sadd", "multi:set", "three")

	c.Check(rd.Command("smembers", "multi:set").Len(), Equals, 3)
}


// Test transactions.
func (s *S) TestTransactions(c *C) { 
	rsA := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "tx:a:string", "Hello, World!")
		mc.Command("get", "tx:a:string")
	})
	c.Check(rsA.ResultSetAt(1).String(), Equals, "Hello, World!")

	rsB := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "tx:b:string", "Hello, World!")
		mc.Command("get", "tx:b:string")
		mc.Discard()
		mc.Command("set", "tx:c:string", "Hello, Redis!")
		mc.Command("get", "tx:c:string")
	})
	c.Check(rsB.ResultSetAt(1).String(), Equals, "Hello, Redis!")

	// Failing transaction
	rsC := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("get", "tx:c:string")
		mc.Command("set", "tx:c:string", "Hello, World!")
		mc.Command("get", "tx:c:string")
	})
	c.Check(rsC.ResultSetAt(2).String(), Not(Equals), "Hello, Redis!")
}

// Test subscribe.
func (s *S) TestSubscribe(c *C) {
	sub, err := rd.Subscribe("subscribe:one", "subscribe:two")
	if err != nil {
		c.Errorf("Can't subscribe: '%v'!", err)
		return
	}

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
	fooPush := func(rd *RedisDatabase) {
		time.Sleep(time.Second)
		rd.Command("lpush", "pop:first", "foo")
	}

	// Set A: no database timeout.
	rdA := NewRedisDatabase(Configuration{})

	go fooPush(rdA)

	rsAA := rdA.Command("blpop", "pop:first", 5)
	kv := rsAA.KeyValue()
	c.Check(kv.Value.String(), Equals, "foo")

	rsAB := rdA.Command("blpop", "pop:first", 1)
	c.Check(rsAB.Error(), NotNil)

	// Set B: database with timeout.
	rdB := NewRedisDatabase(Configuration{})

	rsBA := rdB.Command("blpop", "pop:first", 1)
	c.Check(rsBA.Error(), NotNil)
}

// Test illegal databases.
func (s *Long) TestIllegalDatabases(c *C) {
	c.Log("Test selecting an illegal database...")
	rdA := NewRedisDatabase(Configuration{Database: 4711})
	rsA := rdA.Command("ping")
	c.Check(rsA.Error(), NotNil)

	c.Log("Test connecting to an illegal address...")
	rdB := NewRedisDatabase(Configuration{Address: "192.168.100.100:12345"})
	rsB := rdB.Command("ping")
	c.Check(rsB.Error(), NotNil)
}

// Test database killing with a long run.
func (s *Long) TestDatabaseKill(c *C) {
    rdA := NewRedisDatabase(Configuration{PoolSize: 5})
	
    for i := 1; i < 120; i++ {
        if !rdA.Command("set", "long:run", i).OK() {
            c.Errorf("Long run failed!")
            return
        }
		time.Sleep(time.Second)
    }
}

