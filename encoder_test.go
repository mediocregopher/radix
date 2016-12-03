package radix

import (
	"bytes"
	"errors"
	"fmt"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type textCPMarshaler []byte

func (cm textCPMarshaler) MarshalText() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

type binCPMarshaler []byte

func (cm binCPMarshaler) MarshalBinary() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

type marshaler struct {
	a, b, c string
}

func (m *marshaler) Marshal() (interface{}, error) {
	return []string{m.a, m.b, m.c}, nil
}

// because actually writing this out is a pain, especially for the Cmd tests
func encodeStrArr(ss ...string) string {
	var ret string
	ret = fmt.Sprintf("*%d\r\n", len(ss))
	for _, s := range ss {
		ret += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	}
	return ret
}

var encodeTests = []struct {
	in  interface{}
	out string
}{
	// Bulk strings
	{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
	{in: "ohey", out: "$4\r\nohey\r\n"},
	{in: true, out: "$1\r\n1\r\n"},
	{in: false, out: "$1\r\n0\r\n"},
	{in: nil, out: "$-1\r\n"},
	{in: float32(5.5), out: "$3\r\n5.5\r\n"},
	{in: float64(5.5), out: "$3\r\n5.5\r\n"},
	{in: textCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
	{in: binCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},

	// Int
	{in: 5, out: ":5\r\n"},
	{in: int64(5), out: ":5\r\n"},
	{in: uint64(5), out: ":5\r\n"},

	// Error
	{in: errors.New(":("), out: "-:(\r\n"},

	// Simple arrays
	{in: []string{}, out: "*0\r\n"},
	{in: []string{"a", "b"}, out: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
	{in: []int{1, 2}, out: "*2\r\n:1\r\n:2\r\n"},

	// Complex arrays
	{in: []interface{}{}, out: "*0\r\n"},
	{in: []interface{}{"a", 1}, out: "*2\r\n$1\r\na\r\n:1\r\n"},

	// Embedded arrays
	{
		in:  []interface{}{[]string{"a", "b"}, []int{1, 2}},
		out: "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n:1\r\n:2\r\n",
	},

	// Maps
	{in: map[string]int{"one": 1}, out: "*2\r\n$3\r\none\r\n:1\r\n"},
	{
		in:  map[string]interface{}{"one": []byte("1")},
		out: "*2\r\n$3\r\none\r\n$1\r\n1\r\n",
	},
	{
		in:  map[string]interface{}{"one": []string{"1", "2"}},
		out: "*2\r\n$3\r\none\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n",
	},

	// Marshaler
	{
		in:  &marshaler{a: "aa", b: "bb", c: "cc"},
		out: encodeStrArr("aa", "bb", "cc"),
	},

	// Resp
	{in: Resp{SimpleStr: []byte("")}, out: "+\r\n"},
	{in: Resp{SimpleStr: []byte("ohey")}, out: "+ohey\r\n"},
	{in: Resp{BulkStr: []byte("")}, out: "$0\r\n\r\n"},
	{in: Resp{BulkStr: []byte("ohey")}, out: "$4\r\nohey\r\n"},
	{in: Resp{Err: errors.New("boo")}, out: "-boo\r\n"},
	{in: Resp{BulkStrNil: true}, out: "$-1\r\n"},
	{in: Resp{ArrNil: true}, out: "*-1\r\n"},
	{in: Resp{Arr: []Resp{}}, out: "*0\r\n"},
	{in: Resp{Arr: []Resp{
		Resp{SimpleStr: []byte("ohey")},
		Resp{Int: 5},
	}}, out: "*2\r\n+ohey\r\n:5\r\n"},
	{in: Resp{Int: 0}, out: ":0\r\n"},
	{in: Resp{Int: 5}, out: ":5\r\n"},
	{in: Resp{Int: -5}, out: ":-5\r\n"},

	// Cmd
	{
		in:  CmdNoKey("foo"),
		out: encodeStrArr("foo"),
	},
	{
		in:  Cmd("foo", "bar"),
		out: encodeStrArr("foo", "bar"),
	},
	{
		in:  Cmd("foo", "bar", 1, 7.2),
		out: encodeStrArr("foo", "bar", "1", "7.2"),
	},
	{
		in:  Cmd("foo", "bar", "baz", 1, 7.2),
		out: encodeStrArr("foo", "bar", "baz", "1", "7.2"),
	},
	{
		in:  Cmd("foo", "key", []string{}),
		out: encodeStrArr("foo", "key"),
	},
	{
		in:  Cmd("foo", "key", []string{"bar"}),
		out: encodeStrArr("foo", "key", "bar"),
	},
	{
		in:  Cmd("foo", "key", []string{}, []string{}),
		out: encodeStrArr("foo", "key"),
	},
	{
		in:  Cmd("foo", "key", []string{"bar"}, []string{"baz"}),
		out: encodeStrArr("foo", "key", "bar", "baz"),
	},
	{
		in:  Cmd("foo", "key", []interface{}{}),
		out: encodeStrArr("foo", "key"),
	},
	{
		in:  Cmd("foo", "key", []interface{}{"bar"}),
		out: encodeStrArr("foo", "key", "bar"),
	},
	{
		in:  Cmd("foo", "key", []interface{}{"bar", 1}),
		out: encodeStrArr("foo", "key", "bar", "1"),
	},
	{
		in:  Cmd("foo", "key", []interface{}{"bar", []int{}, []interface{}{}}),
		out: encodeStrArr("foo", "key", "bar"),
	},
	{
		in:  Cmd("foo", "key", []interface{}{"bar", []int{1}}),
		out: encodeStrArr("foo", "key", "bar", "1"),
	},
	{
		in:  Cmd("foo", "key", map[string]int{}),
		out: encodeStrArr("foo", "key"),
	},
	{
		in:  Cmd("foo", "key", map[string]int{"one": 1}),
		out: encodeStrArr("foo", "key", "one", "1"),
	},
	{
		in:  Cmd("foo", "key", map[int]interface{}{1: "one"}),
		out: encodeStrArr("foo", "key", "1", "one"),
	},
}

func TestEncode(t *T) {
	buf := new(bytes.Buffer)
	e := NewEncoder(buf)
	for _, et := range encodeTests {
		require.Nil(t, e.Encode(et.in))
		assert.Equal(t, et.out, buf.String())
		buf.Reset()
	}
}
