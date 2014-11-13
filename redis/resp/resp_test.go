package resp

import (
	"bytes"
	"errors"
	. "launchpad.net/gocheck"
)

type RespSuite struct{}

var _ = Suite(&RespSuite{})

func (_ *RespSuite) TestRead(c *C) {
	var m *Message
	var err error

	_, err = NewMessage(nil)
	c.Check(err, Not(Equals), nil)

	_, err = NewMessage([]byte{})
	c.Check(err, Not(Equals), nil)

	// Simple string
	m, _ = NewMessage([]byte("+ohey\r\n"))
	c.Check(m.Type, Equals, SimpleStr)
	c.Check(m.val.([]byte), DeepEquals, []byte("ohey"))

	// Empty simple string
	m, _ = NewMessage([]byte("+\r\n"))
	c.Check(m.Type, Equals, SimpleStr)
	c.Check(m.val.([]byte), DeepEquals, []byte(""))

	// Error
	m, _ = NewMessage([]byte("-ohey\r\n"))
	c.Check(m.Type, Equals, Err)
	c.Check(m.val.([]byte), DeepEquals, []byte("ohey"))

	// Empty error
	m, _ = NewMessage([]byte("-\r\n"))
	c.Check(m.Type, Equals, Err)
	c.Check(m.val.([]byte), DeepEquals, []byte(""))

	// Int
	m, _ = NewMessage([]byte(":1024\r\n"))
	c.Check(m.Type, Equals, Int)
	c.Check(m.val.(int64), Equals, int64(1024))

	// Bulk string
	m, _ = NewMessage([]byte("$3\r\nfoo\r\n"))
	c.Check(m.Type, Equals, BulkStr)
	c.Check(m.val.([]byte), DeepEquals, []byte("foo"))

	// Empty bulk string
	m, _ = NewMessage([]byte("$0\r\n\r\n"))
	c.Check(m.Type, Equals, BulkStr)
	c.Check(m.val.([]byte), DeepEquals, []byte(""))

	// Nil bulk string
	m, _ = NewMessage([]byte("$-1\r\n"))
	c.Check(m.Type, Equals, Nil)

	// Array
	m, _ = NewMessage([]byte("*2\r\n+foo\r\n+bar\r\n"))
	c.Check(m.Type, Equals, Array)
	c.Check(len(m.val.([]*Message)), Equals, 2)
	c.Check(m.val.([]*Message)[0].Type, Equals, SimpleStr)
	c.Check(m.val.([]*Message)[0].val.([]byte), DeepEquals, []byte("foo"))
	c.Check(m.val.([]*Message)[1].Type, Equals, SimpleStr)
	c.Check(m.val.([]*Message)[1].val.([]byte), DeepEquals, []byte("bar"))

	// Empty array
	m, _ = NewMessage([]byte("*0\r\n"))
	c.Check(m.Type, Equals, Array)
	c.Check(len(m.val.([]*Message)), Equals, 0)

	// Nil Array
	m, _ = NewMessage([]byte("*-1\r\n"))
	c.Check(m.Type, Equals, Nil)

	// Embedded Array
	m, _ = NewMessage([]byte("*3\r\n+foo\r\n+bar\r\n*2\r\n+foo\r\n+bar\r\n"))
	c.Check(m.Type, Equals, Array)
	c.Check(len(m.val.([]*Message)), Equals, 3)
	c.Check(m.val.([]*Message)[0].Type, Equals, SimpleStr)
	c.Check(m.val.([]*Message)[0].val.([]byte), DeepEquals, []byte("foo"))
	c.Check(m.val.([]*Message)[1].Type, Equals, SimpleStr)
	c.Check(m.val.([]*Message)[1].val.([]byte), DeepEquals, []byte("bar"))
	m = m.val.([]*Message)[2]
	c.Check(len(m.val.([]*Message)), Equals, 2)
	c.Check(m.val.([]*Message)[0].Type, Equals, SimpleStr)
	c.Check(m.val.([]*Message)[0].val.([]byte), DeepEquals, []byte("foo"))
	c.Check(m.val.([]*Message)[1].Type, Equals, SimpleStr)
	c.Check(m.val.([]*Message)[1].val.([]byte), DeepEquals, []byte("bar"))

	// Test that two bulks in a row read correctly
	m, _ = NewMessage([]byte("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	c.Check(m.Type, Equals, Array)
	c.Check(len(m.val.([]*Message)), Equals, 2)
	c.Check(m.val.([]*Message)[0].Type, Equals, BulkStr)
	c.Check(m.val.([]*Message)[0].val.([]byte), DeepEquals, []byte("foo"))
	c.Check(m.val.([]*Message)[1].Type, Equals, BulkStr)
	c.Check(m.val.([]*Message)[1].val.([]byte), DeepEquals, []byte("bar"))

}

type arbitraryTest struct {
	val    interface{}
	expect []byte
}

var nilMessage, _ = NewMessage([]byte("$-1\r\n"))

var arbitraryTests = []arbitraryTest{
	{[]byte("OHAI"), []byte("$4\r\nOHAI\r\n")},
	{"OHAI", []byte("$4\r\nOHAI\r\n")},
	{true, []byte("$1\r\n1\r\n")},
	{false, []byte("$1\r\n0\r\n")},
	{nil, []byte("$-1\r\n")},
	{80, []byte(":80\r\n")},
	{int64(-80), []byte(":-80\r\n")},
	{uint64(80), []byte(":80\r\n")},
	{float32(0.1234), []byte("$6\r\n0.1234\r\n")},
	{float64(0.1234), []byte("$6\r\n0.1234\r\n")},
	{errors.New("hi"), []byte("-hi\r\n")},

	{nilMessage, []byte("$-1\r\n")},

	{[]int{1, 2, 3}, []byte("*3\r\n:1\r\n:2\r\n:3\r\n")},
	{map[int]int{1: 2}, []byte("*2\r\n:1\r\n:2\r\n")},
}

var arbitraryAsStringTests = []arbitraryTest{
	{[]byte("OHAI"), []byte("$4\r\nOHAI\r\n")},
	{"OHAI", []byte("$4\r\nOHAI\r\n")},
	{true, []byte("$1\r\n1\r\n")},
	{false, []byte("$1\r\n0\r\n")},
	{nil, []byte("$0\r\n\r\n")},
	{80, []byte("$2\r\n80\r\n")},
	{int64(-80), []byte("$3\r\n-80\r\n")},
	{uint64(80), []byte("$2\r\n80\r\n")},
	{float32(0.1234), []byte("$6\r\n0.1234\r\n")},
	{float64(0.1234), []byte("$6\r\n0.1234\r\n")},
	{errors.New("hi"), []byte("$2\r\nhi\r\n")},

	{nilMessage, []byte("$-1\r\n")},

	{[]int{1, 2, 3}, []byte("*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")},
	{map[int]int{1: 2}, []byte("*2\r\n$1\r\n1\r\n$1\r\n2\r\n")},
}

var arbitraryAsFlattenedStringsTests = []arbitraryTest{
	{
		[]interface{}{"wat", map[string]interface{}{
			"foo": 1,
			"bar": "baz",
		}},
		[]byte("*5\r\n$3\r\nwat\r\n$3\r\nfoo\r\n$1\r\n1\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"),
	},
}

func (_ *RespSuite) TestWriteArbitrary(c *C) {
	var err error
	buf := bytes.NewBuffer([]byte{})
	for _, test := range arbitraryTests {
		c.Logf("Checking test %v", test)
		buf.Reset()
		err = WriteArbitrary(buf, test.val)
		c.Check(err, Equals, nil)
		c.Check(buf.Bytes(), DeepEquals, test.expect)
	}
}

func (_ *RespSuite) TestWriteArbitraryAsString(c *C) {
	var err error
	buf := bytes.NewBuffer([]byte{})
	for _, test := range arbitraryAsStringTests {
		c.Logf("Checking test %v", test)
		buf.Reset()
		err = WriteArbitraryAsString(buf, test.val)
		c.Check(err, Equals, nil)
		c.Check(buf.Bytes(), DeepEquals, test.expect)
	}
}

func (_ *RespSuite) TestWriteArbitraryAsFlattendStrings(c *C) {
	var err error
	buf := bytes.NewBuffer([]byte{})
	for _, test := range arbitraryAsFlattenedStringsTests {
		c.Logf("Checking test %v", test)
		buf.Reset()
		err = WriteArbitraryAsFlattenedStrings(buf, test.val)
		c.Check(err, Equals, nil)
		c.Check(buf.Bytes(), DeepEquals, test.expect)
	}
}

func (_ *RespSuite) TestMessageWrite(c *C) {
	var err error
	var m *Message
	buf := bytes.NewBuffer([]byte{})
	for _, test := range arbitraryTests {
		c.Logf("Checking test; %v", test)
		buf.Reset()
		m, err = NewMessage(test.expect)
		c.Check(err, Equals, nil)
		err = WriteMessage(buf, m)
		c.Check(err, Equals, nil)
		c.Check(buf.Bytes(), DeepEquals, test.expect)
	}
}
