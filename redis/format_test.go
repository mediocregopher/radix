package redis

import (
	. "launchpad.net/gocheck"
)

type FormatSuite struct{}

var _ = Suite(&FormatSuite{})

func (s *FormatSuite) TestFormatArg(c *C) {
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

func (s *FormatSuite) TestCreateRequest(c *C) {
	c.Check(createRequest(&request{cmd: "PING"}), DeepEquals, []byte("*1\r\n$4\r\nPING\r\n"))
	c.Check(createRequest(&request{
		cmd:  "SET",
		args: []interface{}{"key", 5},
	}),
		DeepEquals, []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1\r\n5\r\n"))
}

func (s *FormatSuite) BenchmarkCreateRequest(c *C) {
	for i := 0; i < c.N; i++ {
		createRequest(&request{
			cmd:  "SET",
			args: []interface{}{"key", "bar"},
		})
	}
}

func (s *FormatSuite) BenchmarkCreateRequestSlice(c *C) {
	for i := 0; i < c.N; i++ {
		createRequest(&request{
			cmd:  "SET",
			args: []interface{}{[]string{"key", "bar"}},
		})
	}
}
