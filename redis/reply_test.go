package redis

import (
	. "launchpad.net/gocheck"
)

type ReplySuite struct{}

var _ = Suite(&ReplySuite{})

func (s *ReplySuite) TestStr(c *C) {
	r := &Reply{Type: ErrorReply, Err: ParseError}
	_, err := r.Str()
	c.Check(err, Equals, ParseError)

	r = &Reply{Type: IntegerReply}
	_, err = r.Str()
	c.Check(err, NotNil)

	r = &Reply{Type: StatusReply, buf: []byte("foo")}
	b, err := r.Str()
	c.Check(err, IsNil)
	c.Check(b, Equals, "foo")

	r = &Reply{Type: BulkReply, buf: []byte("foo")}
	b, err = r.Str()
	c.Check(err, IsNil)
	c.Check(b, Equals, "foo")
}

func (s *ReplySuite) TestBytes(c *C) {
	r := &Reply{Type: BulkReply, buf: []byte("foo")}
	b, err := r.Bytes()
	c.Check(err, IsNil)
	c.Check(b, DeepEquals, []byte("foo"))
}

func (s *ReplySuite) TestInt64(c *C) {
	r := &Reply{Type: ErrorReply, Err: ParseError}
	_, err := r.Int64()
	c.Check(err, Equals, ParseError)

	r = &Reply{Type: IntegerReply, int: 5}
	b, err := r.Int64()
	c.Check(err, IsNil)
	c.Check(b, Equals, int64(5))

	r = &Reply{Type: BulkReply, buf: []byte("5")}
	b, err = r.Int64()
	c.Check(err, IsNil)
	c.Check(b, Equals, int64(5))

	r = &Reply{Type: BulkReply, buf: []byte("foo")}
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

	r = &Reply{Type: StatusReply, buf: []byte("0")}
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

func (s *ReplySuite) TestList(c *C) {
	r := &Reply{Type: MultiReply}
	r.Elems = make([]*Reply, 3)
	r.Elems[0] = &Reply{Type: BulkReply, buf: []byte("0")}
	r.Elems[1] = &Reply{Type: NilReply}
	r.Elems[2] = &Reply{Type: BulkReply, buf: []byte("2")}
	l, err := r.List()
	c.Assert(err, IsNil)
	c.Assert(len(l), Equals, 3)
	c.Check(l[0], Equals, "0")
	c.Check(l[1], Equals, "")
	c.Check(l[2], Equals, "2")
}

func (s *ReplySuite) TestBytesList(c *C) {
	r := &Reply{Type: MultiReply}
	r.Elems = make([]*Reply, 3)
	r.Elems[0] = &Reply{Type: BulkReply, buf: []byte("0")}
	r.Elems[1] = &Reply{Type: NilReply}
	r.Elems[2] = &Reply{Type: BulkReply, buf: []byte("2")}
	l, err := r.ListBytes()
	c.Assert(err, IsNil)
	c.Assert(len(l), Equals, 3)
	c.Check(l[0], DeepEquals, []byte("0"))
	c.Assert(l[1], IsNil)
	c.Check(l[2], DeepEquals, []byte("2"))
}

func (s *ReplySuite) TestHash(c *C) {
	r := &Reply{Type: MultiReply}
	r.Elems = make([]*Reply, 6)
	r.Elems[0] = &Reply{Type: BulkReply, buf: []byte("a")}
	r.Elems[1] = &Reply{Type: BulkReply, buf: []byte("0")}
	r.Elems[2] = &Reply{Type: BulkReply, buf: []byte("b")}
	r.Elems[3] = &Reply{Type: NilReply}
	r.Elems[4] = &Reply{Type: BulkReply, buf: []byte("c")}
	r.Elems[5] = &Reply{Type: BulkReply, buf: []byte("2")}
	h, err := r.Hash()
	c.Assert(err, IsNil)
	c.Check(h["a"], Equals, "0")
	c.Check(h["b"], Equals, "")
	c.Check(h["c"], Equals, "2")
}
