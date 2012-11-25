package redis

import (
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

var cn *Conn
var conf Config

type ConnSuite struct{}

func init() {
	conf = DefaultConfig()
	conf.Network = "tcp"
	conf.Address = "127.0.0.1:6379"
	conf.Database = 8
	conf.Timeout = time.Duration(10) * time.Second

	Suite(&ConnSuite{})
}

func (s *ConnSuite) SetUpTest(c *C) {
	var err error
	cn, err = NewConn(conf)
	c.Assert(err, IsNil)
}

func (s *ConnSuite) TearDownTest(c *C) {
	cn.Close()
}

// Test Conn.Call().
func (s *ConnSuite) TestConnCall(c *C) {
	v, _ := cn.Call("echo", "Hello, World!").Str()
	c.Assert(v, Equals, "Hello, World!")
}
