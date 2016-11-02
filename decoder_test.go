package radix

import (
	"bytes"
	"errors"
	"reflect"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type textCPUnmarshaler []byte

func (cu *textCPUnmarshaler) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCPUnmarshaler []byte

func (cu *binCPUnmarshaler) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type unmarshaler struct {
	a, b, c string
}

func (u *unmarshaler) Unmarshal(fn func(interface{}) error) error {
	ss := []*string{
		&u.a,
		&u.b,
		&u.c,
	}
	return fn(&ss)
}

type mapUnmarshaler struct {
	a, b string
}

func (mu *mapUnmarshaler) Unmarshal(fn func(interface{}) error) error {
	m := map[string]*string{
		"a": &mu.a,
		"b": &mu.b,
	}
	return fn(&m)
}

func strPtr(s string) *string {
	return &s
}

func intPtr(i int64) *int64 {
	return &i
}

var decodeTests = []struct {
	in    string
	out   interface{}
	outS  string
	outI  int64
	outF  float64
	Resp  Resp
	isNil bool // nils get special treatment
}{
	// Simple string
	{in: "+\r\n", out: "", outS: "", Resp: Resp{SimpleStr: []byte("")}},
	{in: "+ohey\r\n", out: "ohey", outS: "ohey", Resp: Resp{SimpleStr: []byte("ohey")}},
	{in: "+10\r\n", out: "10", outS: "10", outI: 10, outF: 10, Resp: Resp{SimpleStr: []byte("10")}},
	{in: "+10.5\r\n", out: "10.5", outS: "10.5", outF: 10.5, Resp: Resp{SimpleStr: []byte("10.5")}},

	// Int
	{in: ":1024\r\n", out: int64(1024), outS: "1024", outI: 1024, outF: 1024, Resp: Resp{Int: 1024}},

	//// Bulk string
	{in: "$0\r\n\r\n", out: "", outS: "", Resp: Resp{BulkStr: []byte("")}},
	{in: "$4\r\nohey\r\n", out: "ohey", outS: "ohey", Resp: Resp{BulkStr: []byte("ohey")}},

	// Nils
	{in: "$-1\r\n", out: nil, isNil: true, Resp: Resp{BulkStrNil: true}},
	//{in: "*-1\r\n", out: nil, isNil: true, Resp: Resp{ArrNil: true}},
}

func TestDecode(t *T) {
	buf := new(bytes.Buffer)
	d := NewDecoder(buf)

	for _, dt := range decodeTests {
		doDecode := func(v interface{}) {
			buf.WriteString(dt.in)
			require.Nil(t, d.Decode(v))
		}

		{
			doDecode(nil)
			assert.Zero(t, len(buf.Bytes()))
		}

		{
			w := new(bytes.Buffer)
			doDecode(w)
			assert.Equal(t, dt.outS, w.String())
		}

		// empty interface
		{
			var i interface{}
			doDecode(&i)
			assert.Equal(t, dt.out, i)
		}

		// interface with a string in it already, even the non-string inputs
		// should still scan as strings (except the nils)
		{
			i := interface{}("")
			doDecode(&i)
			if dt.isNil {
				assert.Nil(t, i)
			} else {
				assert.Equal(t, dt.outS, i)
			}
		}

		{
			var s string
			doDecode(&s)
			assert.Equal(t, dt.outS, s)
		}

		if dt.isNil {
			s := new(string)
			*s = "wut"
			doDecode(&s)
			assert.Nil(t, s)
		} else {
			var s *string
			doDecode(&s)
			assert.Equal(t, &dt.outS, s)
		}

		{
			// we make a byte slice and add one bogus character to it, so we can
			// ensure a new slice wasn't allocated
			b := make([]byte, 0, 1024)
			b = append(b, '_')
			bTail := b[1:]
			doDecode(&bTail)
			b = b[:1+len(bTail)] // expand b to encompass the tail
			assert.Equal(t, "_"+dt.outS, string(b))
		}

		{
			// make a byte slice so small it'll _have_ to be reallocated
			// (usually)
			b := make([]byte, 0, 1)
			doDecode(&b)
			assert.Equal(t, dt.outS, string(b))
		}

		{
			var cu textCPUnmarshaler
			doDecode(&cu)
			assert.Equal(t, dt.outS, string(cu))
		}

		{
			var cu binCPUnmarshaler
			doDecode(&cu)
			assert.Equal(t, dt.outS, string(cu))
		}

		if dt.outI > 0 || dt.isNil {
			var i int64
			doDecode(&i)
			assert.Equal(t, dt.outI, i)

			var ui uint64
			doDecode(&ui)
			assert.Equal(t, uint64(dt.outI), ui)
		}

		if dt.outF > 0 || dt.isNil {
			var f32 float32
			doDecode(&f32)
			assert.Equal(t, float32(dt.outF), f32)

			var f64 float64
			doDecode(&f64)
			assert.Equal(t, dt.outF, f64)
		}

		{
			var r Resp
			doDecode(&r)
			assert.Equal(t, dt.Resp, r)
		}
	}
}

var decodeArrayTests = []struct {
	in        string
	into, out interface{}
}{
	// Empty arrays
	{in: "*0\r\n", out: []interface{}{}},
	{in: "*0\r\n", into: []string(nil), out: []string{}},
	{in: "*0\r\n", into: []string{"a"}, out: []string{}},

	// Simple arrays
	{in: "*2\r\n+foo\r\n+bar\r\n", into: nil, out: nil},
	{in: "*2\r\n+foo\r\n+bar\r\n", into: []string(nil), out: []string{"foo", "bar"}},
	{in: "*2\r\n+foo\r\n+bar\r\n", into: []string{"a", "b", "c"}, out: []string{"foo", "bar"}},
	{in: "*2\r\n+foo\r\n+bar\r\n", into: []string{"a"}, out: []string{"foo", "bar"}},
	{in: "*2\r\n+foo\r\n+bar\r\n", into: []*string{}, out: []*string{strPtr("foo"), strPtr("bar")}},
	{in: "*2\r\n+foo\r\n+bar\r\n", into: []*string{strPtr("wut")}, out: []*string{strPtr("foo"), strPtr("bar")}},

	// Simple arrays with integers
	{in: "*2\r\n:1\r\n:2\r\n", into: []int(nil), out: []int{1, 2}},
	{in: "*2\r\n:1\r\n:2\r\n", into: []int{5, 6, 7}, out: []int{1, 2}},
	{in: "*2\r\n:1\r\n:2\r\n", into: []int{5}, out: []int{1, 2}},
	{in: "*2\r\n:1\r\n:2\r\n", into: []string(nil), out: []string{"1", "2"}},

	// Simple arrays into interfaces
	{in: "*2\r\n+foo\r\n+bar\r\n", out: []interface{}{"foo", "bar"}},
	{
		in:   "*2\r\n+1\r\n+2\r\n",
		into: []interface{}{0, 0},
		out:  []interface{}{1, 2},
	},
	{in: "*2\r\n:1\r\n:2\r\n", out: []interface{}{int64(1), int64(2)}},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: []interface{}{"", ""},
		out:  []interface{}{"1", "2"},
	},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: []interface{}{intPtr(0), intPtr(0)},
		out:  []interface{}{intPtr(1), intPtr(2)},
	},

	// Complex array (multiple types)
	{in: "*2\r\n:1\r\n+2\r\n", out: []interface{}{int64(1), "2"}},
	{in: "*2\r\n:1\r\n+2\r\n", into: []interface{}{int64(0), ""}, out: []interface{}{int64(1), "2"}},
	{in: "*2\r\n:1\r\n+2\r\n", into: []interface{}{"", int64(0)}, out: []interface{}{"1", int64(2)}},

	// Embedded (and complex) arrays
	{
		in: "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
		out: []interface{}{
			[]interface{}{"foo", "bar"},
			[]interface{}{"baz"},
		},
	},
	{
		in:   "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
		into: [][]string(nil),
		out:  [][]string{{"foo", "bar"}, {"baz"}},
	},
	{
		in:   "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
		into: [][]string{nil, {"wut", "ok"}, nil},
		out:  [][]string{{"foo", "bar"}, {"baz"}},
	},
	{
		in: "*3\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n:5\r\n:6\r\n",
		out: []interface{}{
			[]interface{}{"foo", "bar"},
			[]interface{}{int64(5)},
			int64(6),
		},
	},
	{
		in: "*4\r\n*2\r\n+foo\r\n+4\r\n*2\r\n+bar\r\n+3\r\n*1\r\n:5\r\n:6\r\n",
		into: []interface{}{
			[]interface{}{nil, int64(0), "2cool4scool"}, // should shrink
			[]interface{}{""},                           // should grow
			nil,                                         // should be created
			"",                                          // should cast
		},
		out: []interface{}{
			[]interface{}{"foo", int64(4)},
			[]interface{}{"bar", "3"},
			[]interface{}{int64(5)},
			"6",
		},
	},

	// TODO Resp arrays

	// Maps
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: map[string]string(nil),
		out:  map[string]string{"1": "2"},
	},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: map[string]string{},
		out:  map[string]string{"1": "2"},
	},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: map[string]string{"1": "5"},
		out:  map[string]string{"1": "2"},
	},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: map[string]int{},
		out:  map[string]int{"1": 2},
	},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: map[int]interface{}{},
		out:  map[int]interface{}{1: int64(2)},
	},
	{
		in:   "*2\r\n:1\r\n:2\r\n",
		into: map[int]interface{}{1: ""},
		out:  map[int]interface{}{1: "2"},
	},
}

func TestDecodeArray(t *T) {
	buf := new(bytes.Buffer)
	d := NewDecoder(buf)
	for _, dt := range decodeArrayTests {
		buf.WriteString(dt.in)
		var into interface{}
		if dt.into != nil {
			dtintov := reflect.ValueOf(dt.into)
			intov := reflect.New(dtintov.Type())
			intov.Elem().Set(dtintov)
			into = intov.Interface()
		} else if dt.out != nil {
			var i interface{}
			into = &i
		}
		require.Nil(t, d.Decode(into))
		if dt.out != nil {
			out := reflect.ValueOf(into).Elem().Interface()
			assert.Equal(t, dt.out, out)
		}
	}
}

func TestUnmarshaler(t *T) {
	u := unmarshaler{}
	buf := bytes.NewBufferString(encodeStrArr("aa", "bb", "cc"))
	d := NewDecoder(buf)
	require.Nil(t, d.Decode(&u))
	assert.Equal(t, unmarshaler{a: "aa", b: "bb", c: "cc"}, u)

	m := mapUnmarshaler{}
	buf.WriteString(encodeStrArr("a", "aa", "b", "bb", "c", "cc"))
	require.Nil(t, d.Decode(&m))
	assert.Equal(t, mapUnmarshaler{a: "aa", b: "bb"}, m)
}

// make sure that when we use a []interface{} to unmarshal into existing
// pointers it doesn't just overwrite them in the interface
func TestDecodePtr(t *T) {
	var a, b int
	buf := bytes.NewBufferString("*2\r\n:1\r\n:2\r\n")
	i := []interface{}{&a, &b}
	require.Nil(t, NewDecoder(buf).Decode(&i))
	assert.Equal(t, 1, a)
	assert.Equal(t, 2, b)
}

// used to test that, in the case of a type decode error, we are still
// discarding the remaining data that was not consumed for that response. this
// applies mostly for arrays, but could also apply for a bulk string
var decodeDiscardTests = []struct {
	in1, in2 string
	into1    interface{} // decode in1 into this, should cause a type error
	out2     interface{} // what we expect out2 to be anyway
}{
	{
		in1:   "$5\r\nhello\r\n",
		in2:   "$5\r\nworld\r\n",
		into1: nil,
		out2:  "world",
	},
	{
		in1:   "$5\r\nhello\r\n",
		in2:   "$5\r\nworld\r\n",
		into1: []string{},
		out2:  "world",
	},
	{
		in1:   "*2\r\n+foo\r\n+bar\r\n",
		in2:   "*2\r\n+baz\r\n+buz\r\n",
		into1: "no dice",
		out2:  []interface{}{"baz", "buz"},
	},
	{
		in1:   "*2\r\n+foo\r\n+bar\r\n",
		in2:   "*2\r\n+baz\r\n+buz\r\n",
		into1: []interface{}{int(0), nil},
		out2:  []interface{}{"baz", "buz"},
	},
	{
		in1:   "*2\r\n+foo\r\n+bar\r\n",
		in2:   "*2\r\n+baz\r\n+buz\r\n",
		into1: map[string]int{},
		out2:  []interface{}{"baz", "buz"},
	},
}

func TestDecodeDiscard(t *T) {
	buf := new(bytes.Buffer)
	d := NewDecoder(buf)
	for _, dt := range decodeDiscardTests {
		buf.WriteString(dt.in1)
		buf.WriteString(dt.in2)
		d.Decode(&dt.into1)
		var i interface{}
		require.Nil(t, d.Decode(&i))
		assert.Equal(t, dt.out2, i)
	}
}

func TestDecodeAppErr(t *T) {
	// AppErr is handled separate from everything else, so it gets its own test
	buf := bytes.NewBufferString("-ohey\r\n")
	d := NewDecoder(buf)
	err := d.Decode(nil)
	assert.Equal(t, AppErr{errors.New("ohey")}, err)
}
