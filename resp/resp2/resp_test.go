package resp2

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
	. "testing"

	errors "golang.org/x/xerrors"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRESPTypes(t *T) {
	newLR := func(s string) resp.LenReader {
		buf := bytes.NewBufferString(s)
		return resp.NewLenReader(buf, int64(buf.Len()))
	}

	type encodeTest struct {
		in  resp.Marshaler
		out string

		errStr bool
	}

	encodeTests := func() []encodeTest {
		return []encodeTest{
			{in: &SimpleString{S: ""}, out: "+\r\n"},
			{in: &SimpleString{S: "foo"}, out: "+foo\r\n"},
			{in: &Error{E: errors.New("")}, out: "-\r\n", errStr: true},
			{in: &Error{E: errors.New("foo")}, out: "-foo\r\n", errStr: true},
			{in: &Int{I: 5}, out: ":5\r\n"},
			{in: &Int{I: 0}, out: ":0\r\n"},
			{in: &Int{I: -5}, out: ":-5\r\n"},
			{in: &BulkStringBytes{B: nil}, out: "$-1\r\n"},
			{in: &BulkStringBytes{B: []byte{}}, out: "$0\r\n\r\n"},
			{in: &BulkStringBytes{B: []byte("foo")}, out: "$3\r\nfoo\r\n"},
			{in: &BulkStringBytes{B: []byte("foo\r\nbar")}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &BulkString{S: ""}, out: "$0\r\n\r\n"},
			{in: &BulkString{S: "foo"}, out: "$3\r\nfoo\r\n"},
			{in: &BulkString{S: "foo\r\nbar"}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &BulkReader{LR: newLR("foo\r\nbar")}, out: "$8\r\nfoo\r\nbar\r\n"},
			{in: &ArrayHeader{N: 5}, out: "*5\r\n"},
			{in: &ArrayHeader{N: -1}, out: "*-1\r\n"},
			{in: &Array{}, out: "*-1\r\n"},
			{in: &Array{A: []resp.Marshaler{}}, out: "*0\r\n"},
			{
				in: &Array{A: []resp.Marshaler{
					SimpleString{S: "foo"},
					Int{I: 5},
				}},
				out: "*2\r\n+foo\r\n:5\r\n",
			},
		}
	}

	for _, et := range encodeTests() {
		buf := new(bytes.Buffer)
		err := et.in.MarshalRESP(buf)
		assert.Nil(t, err)
		assert.Equal(t, et.out, buf.String())

		br := bufio.NewReader(buf)
		umr := reflect.New(reflect.TypeOf(et.in).Elem())
		um, ok := umr.Interface().(resp.Unmarshaler)
		if !ok {
			br.Discard(len(et.out))
			continue
		}

		err = um.UnmarshalRESP(br)
		assert.Nil(t, err)

		var exp interface{} = et.in
		var got interface{} = umr.Interface()
		if et.errStr {
			exp = exp.(error).Error()
			got = got.(error).Error()
		}
		assert.Equal(t, exp, got, "exp:%#v got:%#v", exp, got)
	}

	// Same test, but do all the marshals first, then do all the unmarshals
	{
		ett := encodeTests()
		buf := new(bytes.Buffer)
		for _, et := range ett {
			assert.Nil(t, et.in.MarshalRESP(buf))
		}
		br := bufio.NewReader(buf)
		for _, et := range ett {
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um, ok := umr.Interface().(resp.Unmarshaler)
			if !ok {
				br.Discard(len(et.out))
				continue
			}

			err := um.UnmarshalRESP(br)
			assert.Nil(t, err)

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			}
			assert.Equal(t, exp, got, "exp:%#v got:%#v", exp, got)
		}
	}
}

// structs used for tests
type testStructInner struct {
	Foo int
	bar int
	Baz string `redis:"BAZ"`
	Buz string `redis:"-"`
	Boz *int
}

func intPtr(i int) *int {
	return &i
}

type testStructA struct {
	testStructInner
	Biz []byte
}

type testStructB struct {
	*testStructInner
	Biz []byte
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

		// Structs
		{
			in: testStructA{
				testStructInner: testStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in: testStructB{
				testStructInner: &testStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructB{Biz: []byte("10")},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
	}

	marshal := func(et encodeTest, buf *bytes.Buffer) {
		a := Any{
			I:                     et.in,
			MarshalBulkString:     et.forceStr,
			MarshalNoArrayHeaders: et.flat,
		}
		assert.Nil(t, a.MarshalRESP(buf))
	}

	for _, et := range encodeTests {
		buf := new(bytes.Buffer)
		marshal(et, buf)
		assert.Equal(t, et.out, buf.String(), "et: %#v", et)
	}

	// do them by doing all the marshals at once then reading them all at once
	{
		buf := new(bytes.Buffer)
		for _, et := range encodeTests {
			marshal(et, buf)
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

type lowerCaseUnmarshaler string

func (lcu *lowerCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BulkString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*lcu = lowerCaseUnmarshaler(strings.ToLower(bs.S))
	return nil
}

type upperCaseUnmarshaler string

func (ucu *upperCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BulkString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*ucu = upperCaseUnmarshaler(strings.ToUpper(bs.S))
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
			{in: "$0\r\n\r\n", out: []byte(nil)},
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
			{in: "+\r\n", out: []byte(nil)},
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
			{
				in: "*6\r\n" +
					"$3\r\none\r\n" + "*2\r\n$1\r\n!\r\n$1\r\n1\r\n" +
					"$3\r\ntwo\r\n" + "*2\r\n$2\r\n!!\r\n$1\r\n2\r\n" +
					"$5\r\nthree\r\n" + "*2\r\n$3\r\n!!!\r\n$1\r\n3\r\n",
				out: map[string]map[string]int{
					"one":   {"!": 1},
					"two":   {"!!": 2},
					"three": {"!!!": 3},
				},
			},
			{
				in: "*4\r\n" +
					"$5\r\nhElLo\r\n" + "$5\r\nWoRlD\r\n" +
					"$3\r\nFoO\r\n" + "$3\r\nbAr\r\n",
				out: map[upperCaseUnmarshaler]lowerCaseUnmarshaler{
					"HELLO": "world",
					"FOO":   "bar",
				},
			},

			// Arrays (structs)
			{
				in: "*10\r\n" +
					"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
					"$3\r\nFoo\r\n" + ":1\r\n" +
					"$3\r\nBoz\r\n" + ":100\r\n" +
					"$3\r\nDNE\r\n" + ":1000\r\n" +
					"$3\r\nBiz\r\n" + "$1\r\n5\r\n",
				out: testStructA{
					testStructInner: testStructInner{
						Foo: 1,
						Baz: "3",
						Boz: intPtr(100),
					},
					Biz: []byte("5"),
				},
			},
			{
				in: "*10\r\n" +
					"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
					"$3\r\nBiz\r\n" + "$1\r\n5\r\n" +
					"$3\r\nBoz\r\n" + ":100\r\n" +
					"$3\r\nDNE\r\n" + ":1000\r\n" +
					"$3\r\nFoo\r\n" + ":1\r\n",
				preload: testStructB{testStructInner: new(testStructInner)},
				out: testStructB{
					testStructInner: &testStructInner{
						Foo: 1,
						Baz: "3",
						Boz: intPtr(100),
					},
					Biz: []byte("5"),
				},
			},
		}
	}

	assertUnmarshal := func(br *bufio.Reader, dt decodeTest) {
		debug := []interface{}{
			"preloadEmpty:%v preload:%T(%v) %q -> %T(%v)",
			dt.preloadEmpty, dt.preload, dt.preload, dt.in, dt.out, dt.out,
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

		err := Any{I: into}.UnmarshalRESP(br)
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

	// do reads/writes sequentially
	for _, dt := range decodeTests() {
		br := bufio.NewReader(bytes.NewBufferString(dt.in))
		assertUnmarshal(br, dt)
	}

	// do all the writes before the unmarshals
	{
		buf := new(bytes.Buffer)
		for _, dt := range decodeTests() {
			buf.WriteString(dt.in)
		}
		br := bufio.NewReader(buf)
		for _, dt := range decodeTests() {
			assertUnmarshal(br, dt)
		}
	}
}

func TestRawMessage(t *T) {
	rmtests := []struct {
		b     string
		isNil bool
	}{
		{b: "+\r\n"},
		{b: "+foo\r\n"},
		{b: "-\r\n"},
		{b: "-foo\r\n"},
		{b: ":5\r\n"},
		{b: ":0\r\n"},
		{b: ":-5\r\n"},
		{b: "$-1\r\n", isNil: true},
		{b: "$0\r\n\r\n"},
		{b: "$3\r\nfoo\r\n"},
		{b: "$8\r\nfoo\r\nbar\r\n"},
		{b: "*2\r\n:1\r\n:2\r\n"},
		{b: "*-1\r\n", isNil: true},
	}

	// one at a time
	for _, rmt := range rmtests {
		buf := new(bytes.Buffer)
		{
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(buf))
			assert.Equal(t, rmt.b, buf.String())
			assert.Equal(t, rmt.isNil, rm.IsNil())
		}
		{
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(bufio.NewReader(buf)))
			assert.Equal(t, rmt.b, string(rm))
		}
	}

	// all at once
	{
		buf := new(bytes.Buffer)
		for _, rmt := range rmtests {
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(buf))
		}
		br := bufio.NewReader(buf)
		for _, rmt := range rmtests {
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(br))
			assert.Equal(t, rmt.b, string(rm))
		}
	}
}
