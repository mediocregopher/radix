package redis

import (
	. "launchpad.net/gocheck"
	"time"
)

var conf Config

type ConnSuite struct {
	c *Conn
}

func init() {
	conf = DefaultConfig()
	conf.Database = 8
	conf.Timeout = time.Duration(10) * time.Second

	Suite(&ConnSuite{})
}

func (s *ConnSuite) SetUpTest(c *C) {
	var err error
	s.c, err = Dial("tcp", "127.0.0.1:6379", conf)
	c.Assert(err, IsNil)
}

func (s *ConnSuite) TearDownTest(c *C) {
	s.c.Close()
}

func (s *ConnSuite) TestCall(c *C) {
	v, _ := s.c.Call("echo", "Hello, World!").Str()
	c.Assert(v, Equals, "Hello, World!")
}
/*
func (s *ConnSuite) TestPipeline(c *C) {
	s.c.Append("echo", "foo")
	s.c.Append("echo", "bar")
	s.c.Append("echo", "zot")

	v, _ := s.c.GetReply().Str()
	c.Assert(v, Equals, "foo")

	v, _ = s.c.GetReply().Str()
	c.Assert(v, Equals, "bar")

	v, _ = s.c.GetReply().Str()
	c.Assert(v, Equals, "zot")

	c.Assert(func() { s.c.GetReply() }, PanicMatches, "pipeline queue empty")
}
*/