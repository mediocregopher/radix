package redis

import (
	. "launchpad.net/gocheck"
)

type ReplySuite struct{}
var _ = Suite(&ReplySuite{})

func (s *ReplySuite) TestStr(c *C) {
	r := &Reply{Type: ErrorReply, Err: TimeoutError}
	_, err := r.Str()
	c.Check(err, Equals, TimeoutError)

	r = &Reply{Type: IntegerReply}
	_, err = r.Str()
	c.Check(err, NotNil)

	r = &Reply{Type: StatusReply, str: "foo"}
	b, err := r.Str()
	c.Check(err, IsNil)
	c.Check(b, Equals, "foo")

	r = &Reply{Type: BulkReply, str: "foo"}
	b, err = r.Str()
	c.Check(err, IsNil)
	c.Check(b, Equals, "foo")
}

func (s *ReplySuite) TestBytes(c *C) {
	r := &Reply{Type: BulkReply, str: "foo"}
	b, err := r.Bytes()
	c.Check(err, IsNil)
	c.Check(b, DeepEquals, []byte("foo"))
}

func (s *ReplySuite) TestInt64(c *C) {
	r := &Reply{Type: ErrorReply, Err: TimeoutError}
	_, err := r.Int64()
	c.Check(err, Equals, TimeoutError)

	r = &Reply{Type: IntegerReply, int: 5}
	b, err := r.Int64()
	c.Check(err, IsNil)
	c.Check(b, Equals, int64(5))

	r = &Reply{Type: BulkReply, str: "5"}
	b, err = r.Int64()
	c.Check(err, IsNil)
	c.Check(b, Equals, int64(5))

	r = &Reply{Type: BulkReply, str: "foo"}
	_, err = r.Int64()
	c.Check(err, NotNil)
}

func (s *ReplySuite) TestInt(c *C) {
	r := &Reply{Type: IntegerReply, int: 5}
	b, err := r.Int()
	c.Check(err, IsNil)
	c.Check(b, Equals, 5)
}

func (s *ReplySuite) TestBool(c *C) {
	r := &Reply{Type: IntegerReply, int: 0}
	b, err := r.Bool()
	c.Check(err, IsNil)
	c.Check(b, Equals, false)

	r = &Reply{Type: StatusReply, str: "0"}
	b, err = r.Bool()
	c.Check(err, IsNil)
	c.Check(b, Equals, false)

	r = &Reply{Type: IntegerReply, int: 2}
	b, err = r.Bool()
	c.Check(err, IsNil)
	c.Check(b, Equals, true)

	r = &Reply{Type: NilReply}
	_, err = r.Bool()
	c.Check(err, NotNil)
}

// TODO
func (s *ReplySuite) TestList(c *C) {
}

// TODO
func (s *ReplySuite) TestHash(c *C) {
}
