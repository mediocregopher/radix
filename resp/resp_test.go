package resp

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setPool(p *Pool, vv reflect.Value) {
	vv = reflect.Indirect(vv)
	vv.FieldByName("Pool").Set(reflect.ValueOf(p))
}

func TestRESPTypes(t *T) {
	newLR := func(s string) LenReader {
		buf := bytes.NewBufferString(s)
		return NewLenReader(buf, int64(buf.Len()))
	}

	type encodeTest struct {
		in  Marshaler
		out string
	}

	encodeTests := func(p *Pool) []encodeTest {
		return []encodeTest{
			{in: &SimpleString{S: []byte("")}, out: "+\r\n"},
			{in: &SimpleString{S: []byte("foo")}, out: "+foo\r\n"},
			{in: &Error{E: errors.New("")}, out: "-\r\n"},
			{in: &Error{E: errors.New("foo")}, out: "-foo\r\n"},
			{in: &Int{I: 5}, out: ":5\r\n"},
			{in: &Int{I: 0}, out: ":0\r\n"},
			{in: &Int{I: -5}, out: ":-5\r\n"},
			{in: &BulkString{B: nil}, out: "$-1\r\n"},
			{in: &BulkString{B: []byte{}}, out: "$0\r\n\r\n"},
			{in: &BulkString{B: []byte("foo")}, out: "$3\r\nfoo\r\n"},
			{in: &BulkString{B: []byte("foo\r\nbar")}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &BulkReader{LR: newLR("foo\r\nbar")}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &ArrayHeader{N: 5}, out: "*5\r\n"},
			{in: &ArrayHeader{N: -1}, out: "*-1\r\n"},
		}
	}

	for _, p := range []*Pool{nil, new(Pool)} {
		for _, et := range encodeTests(p) {
			buf := new(bytes.Buffer)
			err := et.in.MarshalRESP(p, buf)
			assert.Nil(t, err)
			assert.Equal(t, et.out, string(buf.Bytes()))

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um, ok := umr.Interface().(Unmarshaler)
			if !ok {
				br.Discard(len(et.out))
				continue
			}

			err = um.UnmarshalRESP(p, br)
			assert.Nil(t, err)
			assert.Equal(t, et.in, umr.Interface())
		}

		// Same test, but do all the marshals first, then do all the unmarshals
		{
			ett := encodeTests(p)
			buf := new(bytes.Buffer)
			for _, et := range ett {
				assert.Nil(t, et.in.MarshalRESP(p, buf))
			}
			br := bufio.NewReader(buf)
			for _, et := range ett {
				umr := reflect.New(reflect.TypeOf(et.in).Elem())
				um, ok := umr.Interface().(Unmarshaler)
				if !ok {
					br.Discard(len(et.out))
					continue
				}

				err := um.UnmarshalRESP(p, br)
				assert.Nil(t, err)
				assert.Equal(t, et.in, umr.Interface())
			}
		}
	}
}

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

func TestAnyMarshal(t *T) {
	type encodeTest struct {
		in             interface{}
		out            string
		forceStr, flat bool
	}

	var encodeTests = []encodeTest{
		// Bulk strings
		{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
		{in: "ohey", out: "$4\r\nohey\r\n"},
		{in: "", out: "$0\r\n\r\n"},
		{in: true, out: "$1\r\n1\r\n"},
		{in: false, out: "$1\r\n0\r\n"},
		{in: nil, out: "$-1\r\n"},
		{in: nil, forceStr: true, out: "$0\r\n\r\n"},
		{in: []byte(nil), out: "$-1\r\n"},
		{in: []byte(nil), forceStr: true, out: "$0\r\n\r\n"},
		{in: float32(5.5), out: "$3\r\n5.5\r\n"},
		{in: float64(5.5), out: "$3\r\n5.5\r\n"},
		{in: textCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: binCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: "ohey", flat: true, out: "$4\r\nohey\r\n"},

		// Int
		{in: 5, out: ":5\r\n"},
		{in: int64(5), out: ":5\r\n"},
		{in: uint64(5), out: ":5\r\n"},
		{in: int64(5), forceStr: true, out: "$1\r\n5\r\n"},
		{in: uint64(5), forceStr: true, out: "$1\r\n5\r\n"},

		// Error
		{in: errors.New(":("), out: "-:(\r\n"},
		{in: errors.New(":("), forceStr: true, out: "$2\r\n:(\r\n"},

		// Simple arrays
		{in: []string(nil), out: "*-1\r\n"},
		{in: []string(nil), flat: true, out: ""},
		{in: []string{}, out: "*0\r\n"},
		{in: []string{}, flat: true, out: ""},
		{in: []string{"a", "b"}, out: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{in: []int{1, 2}, out: "*2\r\n:1\r\n:2\r\n"},
		{in: []int{1, 2}, flat: true, out: ":1\r\n:2\r\n"},
		{in: []int{1, 2}, forceStr: true, out: "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"},
		{in: []int{1, 2}, flat: true, forceStr: true, out: "$1\r\n1\r\n$1\r\n2\r\n"},

		// Complex arrays
		{in: []interface{}{}, out: "*0\r\n"},
		{in: []interface{}{"a", 1}, out: "*2\r\n$1\r\na\r\n:1\r\n"},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			out:      "*2\r\n$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			flat:     true,
			out:      "$1\r\na\r\n$1\r\n1\r\n",
		},

		// Embedded arrays
		{
			in:  []interface{}{[]string{"a", "b"}, []int{1, 2}},
			out: "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n:1\r\n:2\r\n",
		},
		{
			in:   []interface{}{[]string{"a", "b"}, []int{1, 2}},
			flat: true,
			out:  "$1\r\na\r\n$1\r\nb\r\n:1\r\n:2\r\n",
		},

		// Maps
		{in: map[string]int(nil), out: "*-1\r\n"},
		{in: map[string]int(nil), flat: true, out: ""},
		{in: map[string]int{}, out: "*0\r\n"},
		{in: map[string]int{}, flat: true, out: ""},
		{in: map[string]int{"one": 1}, out: "*2\r\n$3\r\none\r\n:1\r\n"},
		{
			in:  map[string]interface{}{"one": []byte("1")},
			out: "*2\r\n$3\r\none\r\n$1\r\n1\r\n",
		},
		{
			in:  map[string]interface{}{"one": []string{"1", "2"}},
			out: "*2\r\n$3\r\none\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:   map[string]interface{}{"one": []string{"1", "2"}},
			flat: true,
			out:  "$3\r\none\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
	}

	marshal := func(et encodeTest, p *Pool, buf *bytes.Buffer) {
		a := Any{
			I:                     et.in,
			MarshalBulkString:     et.forceStr,
			MarshalNoArrayHeaders: et.flat,
		}
		assert.Nil(t, a.MarshalRESP(p, buf))
	}

	// first we do the tests with the same pool each time
	{
		p := new(Pool)
		for _, et := range encodeTests {
			buf := new(bytes.Buffer)
			marshal(et, p, buf)
			assert.Equal(t, et.out, buf.String())
		}
	}

	// then again with a new Pool each time
	for _, et := range encodeTests {
		buf := new(bytes.Buffer)
		marshal(et, nil, buf)
		assert.Equal(t, et.out, buf.String(), "et: %#v", et)
	}

	// do them by doing all the marshals at once then reading them all at once
	{
		buf := new(bytes.Buffer)
		p := new(Pool)
		for _, et := range encodeTests {
			marshal(et, p, buf)
		}
		for _, et := range encodeTests {
			out := buf.Next(len(et.out))
			assert.Equal(t, et.out, string(out))
		}
	}
}

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

type writer []byte

func (w *writer) Write(b []byte) (int, error) {
	*w = append(*w, b...)
	return len(b), nil
}

func TestAnyUnmarshal(t *T) {
	type decodeTest struct {
		in  string
		out interface{}

		// Instead of unmarshalling into a zero-value of out's type, unmarshal
		// into a copy of this, then compare with out
		preload interface{}

		// like preload, but explicitly preload with a pointer to an empty
		// interface
		preloadEmpty bool

		// instead of testing out, test that unmarshal returns an error
		shouldErr string
	}

	decodeTests := func() []decodeTest {
		return []decodeTest{
			// Bulk string
			{in: "$-1\r\n", out: []byte(nil)},
			{in: "$-1\r\n", preload: []byte{1}, out: []byte(nil)},
			{in: "$-1\r\n", preloadEmpty: true, out: []byte(nil)},
			{in: "$0\r\n\r\n", out: ""},
			{in: "$0\r\n\r\n", out: []byte("")},
			{in: "$4\r\nohey\r\n", out: "ohey"},
			{in: "$4\r\nohey\r\n", out: []byte("ohey")},
			{in: "$4\r\nohey\r\n", preload: []byte(nil), out: []byte("ohey")},
			{in: "$4\r\nohey\r\n", preload: []byte(""), out: []byte("ohey")},
			{in: "$4\r\nohey\r\n", preload: []byte("wut"), out: []byte("ohey")},
			{in: "$4\r\nohey\r\n", preload: []byte("wutwut"), out: []byte("ohey")},
			{in: "$4\r\nohey\r\n", out: textCPUnmarshaler("ohey")},
			{in: "$4\r\nohey\r\n", out: binCPUnmarshaler("ohey")},
			{in: "$4\r\nohey\r\n", out: writer("ohey")},
			{in: "$2\r\n10\r\n", out: int(10)},
			{in: "$2\r\n10\r\n", out: uint(10)},
			{in: "$4\r\n10.5\r\n", out: float32(10.5)},
			{in: "$4\r\n10.5\r\n", out: float64(10.5)},
			{in: "$4\r\nohey\r\n", preloadEmpty: true, out: []byte("ohey")},
			{in: "$4\r\nohey\r\n", out: nil},

			// Simple string
			{in: "+\r\n", out: ""},
			{in: "+\r\n", out: []byte("")},
			{in: "+ohey\r\n", out: "ohey"},
			{in: "+ohey\r\n", out: []byte("ohey")},
			{in: "+ohey\r\n", out: textCPUnmarshaler("ohey")},
			{in: "+ohey\r\n", out: binCPUnmarshaler("ohey")},
			{in: "+ohey\r\n", out: writer("ohey")},
			{in: "+10\r\n", out: int(10)},
			{in: "+10\r\n", out: uint(10)},
			{in: "+10.5\r\n", out: float32(10.5)},
			{in: "+10.5\r\n", out: float64(10.5)},
			{in: "+ohey\r\n", preloadEmpty: true, out: "ohey"},
			{in: "+ohey\r\n", out: nil},

			// Err
			{in: "-ohey\r\n", out: "", shouldErr: "ohey"},
			{in: "-ohey\r\n", out: nil, shouldErr: "ohey"},

			// Int
			{in: ":1024\r\n", out: "1024"},
			{in: ":1024\r\n", out: []byte("1024")},
			{in: ":1024\r\n", out: textCPUnmarshaler("1024")},
			{in: ":1024\r\n", out: binCPUnmarshaler("1024")},
			{in: ":1024\r\n", out: writer("1024")},
			{in: ":1024\r\n", out: int(1024)},
			{in: ":1024\r\n", out: uint(1024)},
			{in: ":1024\r\n", out: float32(1024)},
			{in: ":1024\r\n", out: float64(1024)},
			{in: ":1024\r\n", preloadEmpty: true, out: int64(1024)},
			{in: ":1024\r\n", out: nil},

			// Arrays
			{in: "*-1\r\n", out: []interface{}(nil)},
			{in: "*-1\r\n", out: []string(nil)},
			{in: "*-1\r\n", out: map[string]string(nil)},
			{in: "*-1\r\n", preloadEmpty: true, out: []interface{}(nil)},
			{in: "*0\r\n", out: []interface{}{}},
			{in: "*0\r\n", out: []string{}},
			{in: "*0\r\n", preload: map[string]string(nil), out: map[string]string{}},
			{in: "*2\r\n+foo\r\n+bar\r\n", out: []string{"foo", "bar"}},
			{in: "*2\r\n+foo\r\n+bar\r\n", out: []interface{}{"foo", "bar"}},
			{in: "*2\r\n+foo\r\n+bar\r\n", preloadEmpty: true, out: []interface{}{"foo", "bar"}},
			{in: "*2\r\n+foo\r\n+5\r\n", preload: []interface{}{0, 1}, out: []interface{}{"foo", "5"}},
			{
				in: "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
				out: []interface{}{
					[]interface{}{"foo", "bar"},
					[]interface{}{"baz"},
				},
			},
			{
				in:  "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n",
				out: [][]string{{"foo", "bar"}, {"baz"}},
			},
			{
				in:  "*2\r\n*2\r\n+foo\r\n+bar\r\n+baz\r\n",
				out: []interface{}{[]interface{}{"foo", "bar"}, "baz"},
			},
			{in: "*2\r\n:1\r\n:2\r\n", out: map[string]string{"1": "2"}},
			{in: "*2\r\n*2\r\n+foo\r\n+bar\r\n*1\r\n+baz\r\n", out: nil},
		}
	}

	assertUnmarshal := func(br *bufio.Reader, dt decodeTest, p *Pool) {
		debug := []interface{}{
			"withPool:%v preloadEmpty:%v preload:%T(%v) %q -> %T(%v)",
			p != nil, dt.preloadEmpty, dt.preload, dt.preload, dt.in, dt.out, dt.out,
		}
		//t.Logf(debug[0].(string), debug[1:]...)

		var into interface{}
		if dt.preloadEmpty {
			emptyInterfaceT := reflect.TypeOf([]interface{}(nil)).Elem()
			into = reflect.New(emptyInterfaceT).Interface()
		} else if dt.preload != nil {
			intov := reflect.New(reflect.TypeOf(dt.preload))
			intov.Elem().Set(reflect.ValueOf(dt.preload))
			into = intov.Interface()
		} else if dt.out != nil {
			into = reflect.New(reflect.TypeOf(dt.out)).Interface()
		}

		err := Any{I: into}.UnmarshalRESP(p, br)
		if dt.shouldErr != "" {
			require.NotNil(t, err, debug...)
			assert.Equal(t, dt.shouldErr, err.Error(), debug...)
			return
		}

		require.Nil(t, err, debug...)
		if dt.out != nil {
			aI := reflect.ValueOf(into).Elem().Interface()
			assert.Equal(t, dt.out, aI, debug...)
		} else {
			assert.Nil(t, into)
		}
	}

	for _, p := range []*Pool{nil, new(Pool)} {
		// do reads/writes sequentially
		for _, dt := range decodeTests() {
			br := bufio.NewReader(bytes.NewBufferString(dt.in))
			assertUnmarshal(br, dt, p)
		}

		// do all the writes before the unmarshals
		{
			buf := new(bytes.Buffer)
			for _, dt := range decodeTests() {
				buf.WriteString(dt.in)
			}
			br := bufio.NewReader(buf)
			for _, dt := range decodeTests() {
				assertUnmarshal(br, dt, p)
			}
		}
	}
}

func TestRawMessage(t *T) {
	rmtests := []struct {
		b string
	}{
		{b: "+\r\n"},
		{b: "+foo\r\n"},
		{b: "-\r\n"},
		{b: "-foo\r\n"},
		{b: ":5\r\n"},
		{b: ":0\r\n"},
		{b: ":-5\r\n"},
		{b: "$-1\r\n"},
		{b: "$0\r\n\r\n"},
		{b: "$3\r\nfoo\r\n"},
		{b: "$8\r\nfoo\r\nbar\r\n"},
		{b: "*2\r\n:1\r\n:2\r\n"},
		{b: "*-1\r\n"},
	}

	// one at a time
	for _, rmt := range rmtests {
		buf := new(bytes.Buffer)
		{
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(nil, buf))
			assert.Equal(t, rmt.b, buf.String())
		}
		{
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(nil, bufio.NewReader(buf)))
			assert.Equal(t, rmt.b, string(rm))
		}
	}

	// all at once
	{
		buf := new(bytes.Buffer)
		for _, rmt := range rmtests {
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(nil, buf))
		}
		br := bufio.NewReader(buf)
		for _, rmt := range rmtests {
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(nil, br))
			assert.Equal(t, rmt.b, string(rm))
		}
	}
}
