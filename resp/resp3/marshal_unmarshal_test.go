package resp3

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/stretchr/testify/assert"
)

// structs used for tests.
type TestStructInner struct {
	Foo int
	bar int
	Baz string `redis:"BAZ"`
	Buz string `redis:"-"`
	Boz *int64
}

type testStructA struct {
	TestStructInner
	Biz []byte
}

type testStructB struct {
	*TestStructInner
	Biz []byte
}

type textCP []byte

func (cu textCP) MarshalText() ([]byte, error) {
	return []byte(cu), nil
}

func (cu *textCP) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCP []byte

func (cu binCP) MarshalBinary() ([]byte, error) {
	return []byte(cu), nil
}

func (cu *binCP) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type streamedStrRW [][]byte

func (rw *streamedStrRW) Write(b []byte) (int, error) {
	rwB := append([]byte(nil), b...)
	*rw = append(*rw, rwB)
	return len(rwB), nil
}

func (rw streamedStrRW) Read(b []byte) (int, error) {
	// this method jumps through a lot of hoops to not have a pointer receiver,
	// because that makes formulating the tests easier.
	if len(rw) == 0 || len(rw[0]) == 0 {
		return 0, io.EOF
	}

	rwB := rw[0]
	if len(b) < len(rwB) {
		panic("length of byte slice being read into is smaller than what needs to be written")
	}
	copy(b, rwB)
	copy(rw, rw[1:])
	rw[len(rw)-1] = []byte{}
	return len(rwB), nil
}

type msgSeries []interface {
	resp.Marshaler
	resp.Unmarshaler
}

func (s msgSeries) MarshalRESP(w io.Writer, o *resp.Opts) error {
	for i := range s {
		if err := s[i].MarshalRESP(w, o); err != nil {
			return err
		}
	}
	return nil
}

func (s msgSeries) UnmarshalRESP(br resp.BufferedReader, o *resp.Opts) error {
	for i := range s {
		if err := s[i].UnmarshalRESP(br, o); err != nil {
			return err
		}
	}
	return nil
}

type ie [2]interface{} // into/expect

type kase struct { // "case" is reserved
	label string

	// for each kase the input message will be unmarshaled into the first
	// interface{} (the "into") and then compared with the second (the
	// "expected").
	ie ie

	// reverseable, also test AnyMarshaler
	r bool

	// flattenedEmpty indicates that when passed to Flatten the result is
	// expected to be empty. This is only useful in some very specific cases,
	// notably nil slices/maps/structs.
	flattenedEmpty bool
}

type in struct {
	// label is optional and is used to sometimes make generating cases easier
	label string

	// msg(s) are the actual RESP messages being unmarshaled. msg is a
	// convenience field which is equivalent to setting msgs to `[]string{msg}`.
	msg  string
	msgs []string

	// flattened is the msg having been passed to Flatten after unmarshaling.
	flattened []string

	// mkCases generates the test cases specific to this particular input.
	mkCases func() []kase
}

func (in in) mkMsg() string {
	if in.msg != "" {
		return in.msg
	}
	return strings.Join(in.msgs, "")
}

func (in in) prefix() Prefix {
	return Prefix(in.mkMsg()[0])
}

func (in in) streamed() bool {
	return in.mkMsg()[1] == '?'
}

type unmarshalMarshalTest struct {
	descr string
	ins   []in

	// mkCases generates a set of cases in a generic way for each in (useful
	// for cases which are identical or only slightly different across all
	// ins).
	mkCases func(in) []kase

	// instead of testing cases, assert that unmarshal returns this specific
	// error.
	shouldErr error
}

func (umt unmarshalMarshalTest) cases(in in) []kase {
	var cases []kase
	if in.mkCases != nil {
		cases = append(cases, in.mkCases()...)
	}
	if umt.mkCases != nil {
		cases = append(cases, umt.mkCases(in)...)
	}
	for i := range cases {
		if cases[i].label == "" {
			cases[i].label = fmt.Sprintf("case%d", i)
		}
	}
	return cases
}

func TestAnyUnmarshalMarshal(t *testing.T) {

	strPtr := func(s string) *string { return &s }
	bytPtr := func(b []byte) *[]byte { return &b }
	intPtr := func(i int64) *int64 { return &i }
	fltPtr := func(f float64) *float64 { return &f }

	setReversable := func(to bool, kk []kase) []kase {
		for i := range kk {
			kk[i].r = to
		}
		return kk
	}

	strCases := func(in in, str string) []kase {
		prefix := in.prefix()
		kases := setReversable(prefix == BlobStringPrefix, []kase{
			{ie: ie{"", str}},
			{ie: ie{"otherstring", str}},
			{ie: ie{(*string)(nil), strPtr(str)}},
			{ie: ie{strPtr(""), strPtr(str)}},
			{ie: ie{strPtr("otherstring"), strPtr(str)}},
			{ie: ie{[]byte{}, []byte(str)}},
			{ie: ie{[]byte(nil), []byte(str)}},
			{ie: ie{[]byte("f"), []byte(str)}},
			{ie: ie{[]byte("biglongstringblaaaaah"), []byte(str)}},
			{ie: ie{[]rune{}, []rune(str)}},
			{ie: ie{[]rune(nil), []rune(str)}},
			{ie: ie{[]rune("f"), []rune(str)}},
			{ie: ie{[]rune("biglongstringblaaaaah"), []rune(str)}},
			{ie: ie{(*[]byte)(nil), bytPtr([]byte(str))}},
			{ie: ie{bytPtr(nil), bytPtr([]byte(str))}},
			{ie: ie{bytPtr([]byte("f")), bytPtr([]byte(str))}},
			{ie: ie{bytPtr([]byte("biglongstringblaaaaah")), bytPtr([]byte(str))}},
			{ie: ie{textCP{}, textCP(str)}},
			{ie: ie{binCP{}, binCP(str)}},
			{ie: ie{new(bytes.Buffer), bytes.NewBufferString(str)}},
		})
		if prefix == BlobStringPrefix && !in.streamed() {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{BlobString{S: ""}, BlobString{S: str}}},
				{ie: ie{BlobString{S: "f"}, BlobString{S: str}}},
				{ie: ie{BlobString{S: "biglongstringblaaaaah"}, BlobString{S: str}}},
				{ie: ie{BlobStringBytes{B: nil}, BlobStringBytes{B: []byte(str)}}},
				{ie: ie{BlobStringBytes{B: []byte{}}, BlobStringBytes{B: []byte(str)}}},
				{ie: ie{BlobStringBytes{B: []byte("f")}, BlobStringBytes{B: []byte(str)}}},
				{ie: ie{BlobStringBytes{B: []byte("biglongstringblaaaaah")}, BlobStringBytes{B: []byte(str)}}},
			})...)
		} else if prefix == SimpleStringPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{SimpleString{S: ""}, SimpleString{S: str}}},
				{ie: ie{SimpleString{S: "f"}, SimpleString{S: str}}},
				{ie: ie{SimpleString{S: "biglongstringblaaaaah"}, SimpleString{S: str}}},
			})...)
		}
		return kases
	}

	floatCases := func(in in, f float64) []kase {
		prefix := in.prefix()
		kases := setReversable(prefix == DoublePrefix, []kase{
			{ie: ie{float32(0), float32(f)}},
			{ie: ie{float32(1), float32(f)}},
			{ie: ie{float64(0), f}},
			{ie: ie{float64(1), f}},
			{ie: ie{(*float64)(nil), fltPtr(f)}},
			{ie: ie{fltPtr(0), fltPtr(f)}},
			{ie: ie{fltPtr(1), fltPtr(f)}},
			{ie: ie{new(big.Float), new(big.Float).SetFloat64(f)}},
		})
		kases = append(kases, []kase{
			{r: prefix == BooleanPrefix, ie: ie{false, f != 0}},
		}...)
		if prefix == DoublePrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{Double{F: 0}, Double{F: f}}},
				{ie: ie{Double{F: 1.5}, Double{F: f}}},
				{ie: ie{Double{F: -1.5}, Double{F: f}}},
				{ie: ie{Double{F: math.Inf(1)}, Double{F: f}}},
				{ie: ie{Double{F: math.Inf(-1)}, Double{F: f}}},
			})...)

		} else if prefix == BooleanPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{Boolean{B: false}, Boolean{B: f != 0}}},
				{ie: ie{Boolean{B: true}, Boolean{B: f != 0}}},
			})...)
		}
		return kases
	}

	intCases := func(in in, i int64) []kase {
		prefix := in.prefix()
		kases := floatCases(in, float64(i))
		kases = append(kases, setReversable(prefix == NumberPrefix, []kase{
			{ie: ie{int(0), int(i)}},
			{ie: ie{int8(0), int8(i)}},
			{ie: ie{int16(0), int16(i)}},
			{ie: ie{int32(0), int32(i)}},
			{ie: ie{int64(0), i}},
			{ie: ie{int(1), int(i)}},
			{ie: ie{int8(1), int8(i)}},
			{ie: ie{int16(1), int16(i)}},
			{ie: ie{int32(1), int32(i)}},
			{ie: ie{int64(1), i}},
			{ie: ie{(*int64)(nil), intPtr(i)}},
			{ie: ie{intPtr(0), intPtr(i)}},
			{ie: ie{intPtr(1), intPtr(i)}},
		})...)

		kases = append(kases, []kase{
			{
				ie: ie{new(big.Int), new(big.Int).SetInt64(i)},
				r:  prefix == BigNumberPrefix,
			},
		}...)

		if i >= 0 {
			kases = append(kases, setReversable(prefix == NumberPrefix, []kase{
				{ie: ie{uint(0), uint(i)}},
				{ie: ie{uint8(0), uint8(i)}},
				{ie: ie{uint16(0), uint16(i)}},
				{ie: ie{uint32(0), uint32(i)}},
				{ie: ie{uint64(0), uint64(i)}},
				{ie: ie{uint(1), uint(i)}},
				{ie: ie{uint8(1), uint8(i)}},
				{ie: ie{uint16(1), uint16(i)}},
				{ie: ie{uint32(1), uint32(i)}},
				{ie: ie{uint64(1), uint64(i)}},
			})...)
		}

		if prefix == NumberPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{Number{N: 5}, Number{N: i}}},
				{ie: ie{Number{N: 0}, Number{N: i}}},
				{ie: ie{Number{N: -5}, Number{N: i}}},
			})...)

		} else if prefix == BigNumberPrefix {
			kases = append(kases, setReversable(true, []kase{
				{ie: ie{BigNumber{I: new(big.Int)}, BigNumber{I: new(big.Int).SetInt64(i)}}},
				{ie: ie{BigNumber{I: new(big.Int).SetInt64(1)}, BigNumber{I: new(big.Int).SetInt64(i)}}},
				{ie: ie{BigNumber{I: new(big.Int).SetInt64(-1)}, BigNumber{I: new(big.Int).SetInt64(i)}}},
			})...)
		}

		return kases
	}

	nullCases := func(in in) []kase {
		cases := []kase{
			{ie: ie{[]byte(nil), []byte(nil)}},
			{ie: ie{[]byte{}, []byte(nil)}},
			{ie: ie{[]byte{1}, []byte(nil)}},
		}
		cases = append(cases, setReversable(in.prefix() == NullPrefix, []kase{
			{flattenedEmpty: true, ie: ie{[]string(nil), []string(nil)}},
			{flattenedEmpty: true, ie: ie{[]string{}, []string(nil)}},
			{flattenedEmpty: true, ie: ie{[]string{"ohey"}, []string(nil)}},
			{flattenedEmpty: true, ie: ie{map[string]string(nil), map[string]string(nil)}},
			{flattenedEmpty: true, ie: ie{map[string]string{}, map[string]string(nil)}},
			{flattenedEmpty: true, ie: ie{map[string]string{"a": "b"}, map[string]string(nil)}},
			{ie: ie{(*int64)(nil), (*int64)(nil)}},
			{ie: ie{intPtr(0), (*int64)(nil)}},
			{ie: ie{intPtr(1), (*int64)(nil)}},
			{flattenedEmpty: true, ie: ie{(*testStructA)(nil), (*testStructA)(nil)}},
			{flattenedEmpty: true, ie: ie{&testStructA{}, (*testStructA)(nil)}},
			{ie: ie{Null{}, Null{}}},
		})...)
		return cases
	}

	unmarshalMarshalTests := []unmarshalMarshalTest{
		{
			descr: "empty blob string",
			ins: []in{{
				msg:       "$0\r\n\r\n",
				flattened: []string{""},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte{}}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "") },
		},
		{
			descr: "blob string",
			ins: []in{{
				msg:       "$4\r\nohey\r\n",
				flattened: []string{"ohey"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("ohey")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "ohey") },
		},
		{
			descr: "integer blob string",
			ins: []in{{
				msg:       "$2\r\n10\r\n",
				flattened: []string{"10"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("10")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10"), intCases(in, 10)...) },
		},
		{
			descr: "float blob string",
			ins: []in{{
				msg:       "$4\r\n10.5\r\n",
				flattened: []string{"10.5"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("10.5")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10.5"), floatCases(in, 10.5)...) },
		},
		{
			// only for backwards compatibility
			descr: "null blob string",
			ins: []in{{
				msg:       "$-1\r\n",
				flattened: []string{""},
				mkCases: func() []kase {
					return []kase{
						{ie: ie{nil, []byte(nil)}, r: false},
						{ie: ie{BlobStringBytes{}, BlobStringBytes{B: []byte{}}}, r: false},
						{ie: ie{BlobString{}, BlobString{}}, r: false},
					}
				},
			}},
			mkCases: func(in in) []kase { return nullCases(in) },
		},
		{
			descr: "blob string with delim",
			ins: []in{{
				msg:       "$6\r\nab\r\ncd\r\n",
				flattened: []string{"ab\r\ncd"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("ab\r\ncd")}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "ab\r\ncd") },
		},
		{
			descr: "empty simple string",
			ins: []in{{
				msg:       "+\r\n",
				flattened: []string{""},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, ""}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "") },
		},
		{
			descr: "simple string",
			ins: []in{{
				msg:       "+ohey\r\n",
				flattened: []string{"ohey"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, "ohey"}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return strCases(in, "ohey") },
		},
		{
			descr: "integer simple string",
			ins: []in{{
				msg:       "+10\r\n",
				flattened: []string{"10"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, "10"}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10"), intCases(in, 10)...) },
		},
		{
			descr: "float simple string",
			ins: []in{{
				msg:       "+10.5\r\n",
				flattened: []string{"10.5"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, "10.5"}, r: false}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10.5"), floatCases(in, 10.5)...) },
		},
		{
			descr: "empty simple error",
			ins: []in{{
				msg:       "-\r\n",
				flattened: []string{""},
			}},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: ""}},
		},
		{
			descr: "simple error",
			ins: []in{{
				msg:       "-ohey\r\n",
				flattened: []string{"ohey"},
			}},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: "ohey"}},
		},
		{
			descr: "zero number",
			ins: []in{{
				msg:       ":0\r\n",
				flattened: []string{"0"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, int64(0)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "0"), intCases(in, 0)...) },
		},
		{
			descr: "positive number",
			ins: []in{{
				msg:       ":10\r\n",
				flattened: []string{"10"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, int64(10)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10"), intCases(in, 10)...) },
		},
		{
			descr: "negative number",
			ins: []in{{
				msg:       ":-10\r\n",
				flattened: []string{"-10"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, int64(-10)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "-10"), intCases(in, -10)...) },
		},
		{
			descr: "null",
			ins: []in{{
				msg:       "_\r\n",
				flattened: []string{""},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, nil}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return nullCases(in) },
		},
		{
			descr: "zero double",
			ins: []in{{
				msg:       ",0\r\n",
				flattened: []string{"0"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, float64(0)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "0"), floatCases(in, 0)...) },
		},
		{
			descr: "positive double",
			ins: []in{{
				msg:       ",10.5\r\n",
				flattened: []string{"10.5"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, float64(10.5)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "10.5"), floatCases(in, 10.5)...) },
		},
		{
			descr: "positive double infinity",
			ins: []in{{
				msg:       ",inf\r\n",
				flattened: []string{"inf"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, math.Inf(1)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "inf"), floatCases(in, math.Inf(1))...) },
		},
		{
			descr: "negative double",
			ins: []in{{
				msg:       ",-10.5\r\n",
				flattened: []string{"-10.5"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, float64(-10.5)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "-10.5"), floatCases(in, -10.5)...) },
		},
		{
			descr: "negative double infinity",
			ins: []in{{
				msg:       ",-inf\r\n",
				flattened: []string{"-inf"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, math.Inf(-1)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase {
				return append(strCases(in, "-inf"), floatCases(in, math.Inf(-1))...)
			},
		},
		{
			descr: "true",
			ins: []in{{
				msg:       "#t\r\n",
				flattened: []string{"1"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, true}, r: true}}
				},
			}},
			// intCases will include actually unmarshaling into a bool
			mkCases: func(in in) []kase { return append(strCases(in, "1"), intCases(in, 1)...) },
		},
		{
			descr: "false",
			ins: []in{{
				msg:       "#f\r\n",
				flattened: []string{"0"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, false}, r: true}}
				},
			}},
			// intCases will include actually unmarshaling into a bool
			mkCases: func(in in) []kase { return append(strCases(in, "0"), intCases(in, 0)...) },
		},
		{
			descr: "empty blob error",
			ins: []in{{
				msg:       "!0\r\n\r\n",
				flattened: []string{""},
			}},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte{}}},
		},
		{
			descr: "blob error",
			ins: []in{{
				msg:       "!4\r\nohey\r\n",
				flattened: []string{"ohey"},
			}},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("ohey")}},
		},
		{
			descr: "blob error with delim",
			ins: []in{{
				msg:       "!6\r\noh\r\ney\r\n",
				flattened: []string{"oh\r\ney"},
			}},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("oh\r\ney")}},
		},
		{
			descr: "empty verbatim string",
			ins: []in{{
				msg:       "=4\r\ntxt:\r\n",
				flattened: []string{""},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("")}, r: false}}
				},
			}},
			mkCases: func(in in) []kase {
				cases := strCases(in, "")
				cases = append(cases, setReversable(true, []kase{
					{ie: ie{VerbatimString{}, VerbatimString{Format: "txt"}}},
					{ie: ie{
						VerbatimString{Format: "foo", S: "bar"},
						VerbatimString{Format: "txt"},
					}},
					{ie: ie{VerbatimStringBytes{}, VerbatimStringBytes{Format: []byte("txt"), B: []byte{}}}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte{}},
					}},
				})...)
				return cases
			},
		},
		{
			descr: "verbatim string",
			ins: []in{{
				msg:       "=8\r\ntxt:ohey\r\n",
				flattened: []string{"ohey"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("ohey")}, r: false}}
				},
			}},
			mkCases: func(in in) []kase {
				cases := strCases(in, "ohey")
				cases = append(cases, setReversable(true, []kase{
					{ie: ie{
						VerbatimString{},
						VerbatimString{Format: "txt", S: "ohey"},
					}},
					{ie: ie{
						VerbatimString{Format: "foo", S: "bar"},
						VerbatimString{Format: "txt", S: "ohey"},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("ohey")},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("ohey")},
					}},
				})...)
				return cases
			},
		},
		{
			descr: "verbatim string with delim",
			ins: []in{{
				msg:       "=10\r\ntxt:oh\r\ney\r\n",
				flattened: []string{"oh\r\ney"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, []byte("oh\r\ney")}, r: false}}
				},
			}},
			mkCases: func(in in) []kase {
				cases := strCases(in, "oh\r\ney")
				cases = append(cases, setReversable(true, []kase{
					{ie: ie{
						VerbatimString{},
						VerbatimString{Format: "txt", S: "oh\r\ney"},
					}},
					{ie: ie{
						VerbatimString{Format: "foo", S: "bar"},
						VerbatimString{Format: "txt", S: "oh\r\ney"},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("oh\r\ney")},
					}},
					{ie: ie{
						VerbatimStringBytes{Format: []byte("foo"), B: []byte("bar")},
						VerbatimStringBytes{Format: []byte("txt"), B: []byte("oh\r\ney")},
					}},
				})...)
				return cases
			},
		},
		{
			descr: "zero big number",
			ins: []in{{
				msg:       "(0\r\n",
				flattened: []string{"0"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, new(big.Int)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "0"), intCases(in, 0)...) },
		},
		{
			descr: "positive big number",
			ins: []in{{
				msg:       "(1000\r\n",
				flattened: []string{"1000"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, new(big.Int).SetInt64(1000)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "1000"), intCases(in, 1000)...) },
		},
		{
			descr: "negative big number",
			ins: []in{{
				msg:       "(-1000\r\n",
				flattened: []string{"-1000"},
				mkCases: func() []kase {
					return []kase{{ie: ie{nil, new(big.Int).SetInt64(-1000)}, r: true}}
				},
			}},
			mkCases: func(in in) []kase { return append(strCases(in, "-1000"), intCases(in, -1000)...) },
		},
		{
			// only for backwards compatibility
			descr: "null array",
			ins: []in{{
				msg:       "*-1\r\n",
				flattened: []string{""},
				mkCases: func() []kase {
					return []kase{
						{ie: ie{nil, []interface{}(nil)}, r: false, flattenedEmpty: true},
						{ie: ie{ArrayHeader{}, ArrayHeader{}}, r: false, flattenedEmpty: true},
					}
				},
			}},
			mkCases: func(in in) []kase { return nullCases(in) },
		},
		{
			descr: "empty agg",
			ins: []in{
				{
					msg:       "*0\r\n",
					flattened: []string{},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{}}},
							{ie: ie{ArrayHeader{}, ArrayHeader{}}},
							{ie: ie{ArrayHeader{NumElems: 1}, ArrayHeader{}}},
						})
					},
				},
				{
					msgs:      []string{"*?\r\n", ".\r\n"},
					flattened: []string{},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{}}},
							{r: true, ie: ie{
								msgSeries{
									&ArrayHeader{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&ArrayHeader{StreamedArrayHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&ArrayHeader{NumElems: 5},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&ArrayHeader{StreamedArrayHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:       "~0\r\n",
					flattened: []string{},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]struct{}{}}},
							{ie: ie{SetHeader{}, SetHeader{}}},
							{ie: ie{SetHeader{NumElems: 1}, SetHeader{}}},
						})
					},
				},
				{
					msgs:      []string{"~?\r\n", ".\r\n"},
					flattened: []string{},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]struct{}{}}},
							{r: true, ie: ie{
								msgSeries{
									&SetHeader{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&SetHeader{StreamedSetHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&SetHeader{NumElems: 5},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&SetHeader{StreamedSetHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:       "%0\r\n",
					flattened: []string{},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]interface{}{}}},
							{ie: ie{MapHeader{}, MapHeader{}}},
							{ie: ie{MapHeader{NumPairs: 1}, MapHeader{}}},
						})
					},
				},
				{
					msgs:      []string{"%?\r\n", ".\r\n"},
					flattened: []string{},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]interface{}{}}},
							{r: true, ie: ie{
								msgSeries{
									&MapHeader{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&MapHeader{StreamedMapHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&MapHeader{NumPairs: 5},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&MapHeader{StreamedMapHeader: true},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				prefix := in.prefix()
				streamed := in.streamed()
				isArray := prefix == ArrayHeaderPrefix
				isSet := prefix == SetHeaderPrefix
				isMap := prefix == MapHeaderPrefix
				return []kase{
					{r: !streamed && isArray, ie: ie{[][]byte(nil), [][]byte{}}},
					{r: !streamed && isArray, ie: ie{[][]byte{}, [][]byte{}}},
					{r: !streamed && isArray, ie: ie{[][]byte{[]byte("a")}, [][]byte{}}},
					{r: !streamed && isArray, ie: ie{[]string(nil), []string{}}},
					{r: !streamed && isArray, ie: ie{[]string{}, []string{}}},
					{r: !streamed && isArray, ie: ie{[]string{"a"}, []string{}}},
					{r: !streamed && isArray, ie: ie{[]int(nil), []int{}}},
					{r: !streamed && isArray, ie: ie{[]int{}, []int{}}},
					{r: !streamed && isArray, ie: ie{[]int{5}, []int{}}},
					{r: !streamed && isSet, ie: ie{map[string]struct{}(nil), map[string]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[string]struct{}{}, map[string]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[string]struct{}{"a": {}}, map[string]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[int]struct{}(nil), map[int]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[int]struct{}{}, map[int]struct{}{}}},
					{r: !streamed && isSet, ie: ie{map[int]struct{}{1: {}}, map[int]struct{}{}}},
					{r: !streamed && isMap, ie: ie{map[string][]byte(nil), map[string][]byte{}}},
					{r: !streamed && isMap, ie: ie{map[string][]byte{}, map[string][]byte{}}},
					{r: !streamed && isMap, ie: ie{map[string][]byte{"a": []byte("b")}, map[string][]byte{}}},
					{r: !streamed && isMap, ie: ie{map[string]string(nil), map[string]string{}}},
					{r: !streamed && isMap, ie: ie{map[string]string{}, map[string]string{}}},
					{r: !streamed && isMap, ie: ie{map[string]string{"a": "b"}, map[string]string{}}},
					{r: !streamed && isMap, ie: ie{map[int]int(nil), map[int]int{}}},
					{r: !streamed && isMap, ie: ie{map[int]int{}, map[int]int{}}},
					{r: !streamed && isMap, ie: ie{map[int]int{5: 5}, map[int]int{}}},
				}
			},
		},
		{
			descr: "two element agg",
			ins: []in{
				{
					msg:       "*2\r\n$1\r\n1\r\n$3\r\n666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{[]byte("1"), []byte("666")}}},
							{ie: ie{
								msgSeries{&ArrayHeader{}, &BlobString{}, &BlobString{}},
								msgSeries{
									&ArrayHeader{NumElems: 2},
									&BlobString{S: "1"},
									&BlobString{S: "666"},
								},
							}},
						})
					},
				},
				{
					msgs: []string{
						"*?\r\n",
						"$1\r\n1\r\n", ":666\r\n",
						".\r\n",
					},
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]byte("1"), int64(666)}}},
							{r: true, ie: ie{
								msgSeries{
									&ArrayHeader{},
									&BlobString{},
									&Number{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&ArrayHeader{StreamedArrayHeader: true},
									&BlobString{S: "1"},
									&Number{N: 666},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:       "~2\r\n:1\r\n:666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]struct{}{int64(666): {}, int64(1): {}}}},
							{ie: ie{
								msgSeries{&SetHeader{}, &Number{}, &Number{}},
								msgSeries{
									&SetHeader{NumElems: 2},
									&Number{N: 1},
									&Number{N: 666},
								},
							}},
						})
					},
				},
				{
					msgs: []string{
						"~?\r\n",
						":1\r\n", ":666\r\n",
						".\r\n",
					},
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]struct{}{int64(1): {}, int64(666): {}}}},
							{r: true, ie: ie{
								msgSeries{
									&SetHeader{},
									&Number{},
									&Number{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&SetHeader{StreamedSetHeader: true},
									&Number{N: 1},
									&Number{N: 666},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:       "%1\r\n:1\r\n:666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, map[interface{}]interface{}{int64(1): int64(666)}}},
							{ie: ie{
								msgSeries{&MapHeader{}, &Number{}, &Number{}},
								msgSeries{
									&MapHeader{NumPairs: 1},
									&Number{N: 1},
									&Number{N: 666},
								},
							}},
						})
					},
				},
				{
					msgs: []string{
						"%?\r\n",
						",1\r\n", ":666\r\n",
						".\r\n",
					},
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]interface{}{float64(1): int64(666)}}},
							{r: true, ie: ie{
								msgSeries{
									&MapHeader{},
									&Double{},
									&Number{},
									&StreamedAggregatedTypeEnd{},
								},
								msgSeries{
									&MapHeader{StreamedMapHeader: true},
									&Double{F: 1},
									&Number{N: 666},
									&StreamedAggregatedTypeEnd{},
								},
							}},
						}
					},
				},
				{
					msg:       ">2\r\n+1\r\n:666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{"1", int64(666)}}},
							{r: true, ie: ie{
								msgSeries{&PushHeader{}, &SimpleString{}, &Number{}},
								msgSeries{
									&PushHeader{NumElems: 2},
									&SimpleString{S: "1"},
									&Number{N: 666},
								},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				prefix := in.prefix()
				streamed := in.streamed()
				return []kase{
					{r: !streamed && prefix == ArrayHeaderPrefix, ie: ie{[][]byte(nil), [][]byte{[]byte("1"), []byte("666")}}},
					{r: !streamed && prefix == ArrayHeaderPrefix, ie: ie{[][]byte{}, [][]byte{[]byte("1"), []byte("666")}}},
					{r: !streamed && prefix == ArrayHeaderPrefix, ie: ie{[][]byte{[]byte("a")}, [][]byte{[]byte("1"), []byte("666")}}},
					{r: !streamed && prefix == ArrayHeaderPrefix, ie: ie{[]string(nil), []string{"1", "666"}}},
					{r: !streamed && prefix == ArrayHeaderPrefix, ie: ie{[]string{}, []string{"1", "666"}}},
					{r: !streamed && prefix == ArrayHeaderPrefix, ie: ie{[]string{"a"}, []string{"1", "666"}}},
					{ie: ie{[]int(nil), []int{1, 666}}},
					{ie: ie{[]int{}, []int{1, 666}}},
					{ie: ie{[]int{5}, []int{1, 666}}},
					{ie: ie{map[string]struct{}(nil), map[string]struct{}{"666": {}, "1": {}}}},
					{ie: ie{map[string]struct{}{}, map[string]struct{}{"666": {}, "1": {}}}},
					{ie: ie{map[string]struct{}{"a": {}}, map[string]struct{}{"666": {}, "1": {}}}},
					{r: !streamed && prefix == SetHeaderPrefix, ie: ie{map[int]struct{}(nil), map[int]struct{}{666: {}, 1: {}}}},
					{r: !streamed && prefix == SetHeaderPrefix, ie: ie{map[int]struct{}{}, map[int]struct{}{666: {}, 1: {}}}},
					{r: !streamed && prefix == SetHeaderPrefix, ie: ie{map[int]struct{}{1: {}}, map[int]struct{}{666: {}, 1: {}}}},
					{ie: ie{map[string][]byte(nil), map[string][]byte{"1": []byte("666")}}},
					{ie: ie{map[string][]byte{}, map[string][]byte{"1": []byte("666")}}},
					{ie: ie{map[string][]byte{"a": []byte("b")}, map[string][]byte{"1": []byte("666")}}},
					{ie: ie{map[string]string(nil), map[string]string{"1": "666"}}},
					{ie: ie{map[string]string{}, map[string]string{"1": "666"}}},
					{ie: ie{map[string]string{"a": "b"}, map[string]string{"1": "666"}}},
					{r: !streamed && prefix == MapHeaderPrefix, ie: ie{map[int]int(nil), map[int]int{1: 666}}},
					{r: !streamed && prefix == MapHeaderPrefix, ie: ie{map[int]int{}, map[int]int{1: 666}}},
					{r: !streamed && prefix == MapHeaderPrefix, ie: ie{map[int]int{5: 5}, map[int]int{1: 666}}},
					{ie: ie{map[string]int(nil), map[string]int{"1": 666}}},
					{ie: ie{map[string]int{}, map[string]int{"1": 666}}},
					{ie: ie{map[string]int{"5": 5}, map[string]int{"1": 666}}},
				}
			},
		},
		{
			descr: "nested two element agg",
			ins: []in{
				{
					label:     "arr-arr",
					msg:       "*1\r\n*2\r\n$1\r\n1\r\n$3\r\n666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{[]interface{}{[]byte("1"), []byte("666")}}}},
						})
					},
				},
				{
					msgs: []string{
						"*?\r\n",
						"*2\r\n$1\r\n1\r\n:666\r\n",
						".\r\n",
					},
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
				{
					label:     "arr-set",
					msg:       "*1\r\n~2\r\n:1\r\n:666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{
								map[interface{}]struct{}{int64(666): {}, int64(1): {}},
							}}},
						})
					},
				},
				{
					msgs: []string{
						"*?\r\n",
						"~2\r\n:1\r\n:666\r\n",
						".\r\n",
					},
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{map[interface{}]struct{}{int64(1): {}, int64(666): {}}}}},
						}
					},
				},
				{
					label:     "arr-map",
					msg:       "*1\r\n%1\r\n:1\r\n$3\r\n666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{map[interface{}]interface{}{int64(1): []byte("666")}}}},
						})
					},
				},
				{
					label:     "arr-map-simple",
					msg:       "*1\r\n%1\r\n:1\r\n$3\r\n666\r\n",
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return setReversable(true, []kase{
							{ie: ie{nil, []interface{}{map[interface{}]interface{}{int64(1): []byte("666")}}}},
						})
					},
				},
				{
					msgs: []string{
						"*?\r\n",
						"%1\r\n:1\r\n$3\r\n666\r\n",
						".\r\n",
					},
					flattened: []string{"1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{map[interface{}]interface{}{int64(1): []byte("666")}}}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				return []kase{
					{r: in.label == "arr-arr", ie: ie{[][][]byte(nil), [][][]byte{{[]byte("1"), []byte("666")}}}},
					{r: in.label == "arr-arr", ie: ie{[][][]byte{}, [][][]byte{{[]byte("1"), []byte("666")}}}},
					{r: in.label == "arr-arr", ie: ie{[][][]byte{{}, {[]byte("a")}}, [][][]byte{{[]byte("1"), []byte("666")}}}},
					{r: in.label == "arr-arr", ie: ie{[][]string(nil), [][]string{{"1", "666"}}}},
					{r: in.label == "arr-arr", ie: ie{[][]string{}, [][]string{{"1", "666"}}}},
					{r: in.label == "arr-arr", ie: ie{[][]string{{}, {"a"}}, [][]string{{"1", "666"}}}},
					{ie: ie{[][]int(nil), [][]int{{1, 666}}}},
					{ie: ie{[][]int{}, [][]int{{1, 666}}}},
					{ie: ie{[][]int{{7}, {5}}, [][]int{{1, 666}}}},
					{ie: ie{[]map[string]struct{}(nil), []map[string]struct{}{{"666": {}, "1": {}}}}},
					{ie: ie{[]map[string]struct{}{}, []map[string]struct{}{{"666": {}, "1": {}}}}},
					{ie: ie{[]map[string]struct{}{{"a": {}}}, []map[string]struct{}{{"666": {}, "1": {}}}}},
					{r: in.label == "arr-set", ie: ie{[]map[int]struct{}(nil), []map[int]struct{}{{666: {}, 1: {}}}}},
					{r: in.label == "arr-set", ie: ie{[]map[int]struct{}{}, []map[int]struct{}{{666: {}, 1: {}}}}},
					{r: in.label == "arr-set", ie: ie{[]map[int]struct{}{{1: {}}}, []map[int]struct{}{{666: {}, 1: {}}}}},
					{ie: ie{[]map[string][]byte(nil), []map[string][]byte{{"1": []byte("666")}}}},
					{ie: ie{[]map[string][]byte{}, []map[string][]byte{{"1": []byte("666")}}}},
					{ie: ie{[]map[string][]byte{{}, {"a": []byte("b")}}, []map[string][]byte{{"1": []byte("666")}}}},
					{ie: ie{[]map[string]string(nil), []map[string]string{{"1": "666"}}}},
					{ie: ie{[]map[string]string{}, []map[string]string{{"1": "666"}}}},
					{ie: ie{[]map[string]string{{}, {"a": "b"}}, []map[string]string{{"1": "666"}}}},
					{ie: ie{[]map[int]int(nil), []map[int]int{{1: 666}}}},
					{ie: ie{[]map[int]int{}, []map[int]int{{1: 666}}}},
					{ie: ie{[]map[int]int{{4: 2}, {7: 5}}, []map[int]int{{1: 666}}}},
					{r: in.label == "arr-map-simple", ie: ie{[]map[int]string(nil), []map[int]string{{1: "666"}}}},
					{r: in.label == "arr-map-simple", ie: ie{[]map[int]string{}, []map[int]string{{1: "666"}}}},
					{r: in.label == "arr-map-simple", ie: ie{[]map[int]string{{4: "2"}, {7: "5"}}, []map[int]string{{1: "666"}}}},
				}
			},
		},
		{
			descr: "keyed nested two element agg",
			ins: []in{
				{
					msg:       "*2\r\n$2\r\n10\r\n*2\r\n$1\r\n1\r\n:666\r\n",
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{nil, []interface{}{[]byte("10"), []interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
				{
					msgs: []string{
						"*?\r\n",
						"$2\r\n10\r\n",
						"*2\r\n$1\r\n1\r\n:666\r\n",
						".\r\n",
					},
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]byte("10"), []interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
				{
					msg:       "*2\r\n$2\r\n10\r\n~2\r\n:1\r\n:666\r\n",
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{nil, []interface{}{
								[]byte("10"),
								map[interface{}]struct{}{int64(666): {}, int64(1): {}},
							}}},
						}
					},
				},
				{
					msgs: []string{
						"*?\r\n",
						"$2\r\n10\r\n",
						"~2\r\n:1\r\n:666\r\n",
						".\r\n",
					},
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{
								[]byte("10"),
								map[interface{}]struct{}{int64(666): {}, int64(1): {}},
							}}},
						}
					},
				},
				{
					msg:       "%1\r\n:10\r\n%1\r\n:1\r\n$3\r\n666\r\n",
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{nil, map[interface{}]interface{}{
								int64(10): map[interface{}]interface{}{int64(1): []byte("666")},
							}}},
						}
					},
				},
				{
					msgs: []string{
						"%?\r\n",
						":10\r\n",
						"%1\r\n:1\r\n$3\r\n666\r\n",
						".\r\n",
					},
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, map[interface{}]interface{}{
								int64(10): map[interface{}]interface{}{int64(1): []byte("666")},
							}}},
						}
					},
				},
				{
					msg:       ">2\r\n$2\r\n10\r\n*2\r\n$1\r\n1\r\n:666\r\n",
					flattened: []string{"10", "1", "666"},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []interface{}{[]byte("10"), []interface{}{[]byte("1"), int64(666)}}}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				prefix := in.prefix()
				streamed := in.streamed()
				return []kase{
					{ie: ie{map[string]map[string][]byte(nil), map[string]map[string][]byte{"10": {"1": []byte("666")}}}},
					{ie: ie{map[string]map[string][]byte{}, map[string]map[string][]byte{"10": {"1": []byte("666")}}}},
					{ie: ie{map[string]map[string][]byte{"foo": {"a": []byte("b")}}, map[string]map[string][]byte{"10": {"1": []byte("666")}}}},
					{ie: ie{map[string]map[string]string(nil), map[string]map[string]string{"10": {"1": "666"}}}},
					{ie: ie{map[string]map[string]string{}, map[string]map[string]string{"10": {"1": "666"}}}},
					{ie: ie{map[string]map[string]string{"foo": {"a": "b"}}, map[string]map[string]string{"10": {"1": "666"}}}},
					{r: !streamed && prefix == MapHeaderPrefix, ie: ie{map[int]map[int]string(nil), map[int]map[int]string{10: {1: "666"}}}},
					{r: !streamed && prefix == MapHeaderPrefix, ie: ie{map[int]map[int]string{}, map[int]map[int]string{10: {1: "666"}}}},
					{r: !streamed && prefix == MapHeaderPrefix, ie: ie{map[int]map[int]string{777: {4: "2"}}, map[int]map[int]string{10: {1: "666"}}}},
					{ie: ie{map[int]map[int]int(nil), map[int]map[int]int{10: {1: 666}}}},
					{ie: ie{map[int]map[int]int{}, map[int]map[int]int{10: {1: 666}}}},
					{ie: ie{map[int]map[int]int{5: {4: 2}}, map[int]map[int]int{10: {1: 666}}}},
				}
			},
		},
		{
			descr: "agg into struct",
			ins: []in{
				{
					label:     "arr",
					msg:       "*10\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
				{
					label:     "arr-bulk-fields",
					msg:       "*10\r\n$3\r\nFoo\r\n:1\r\n$3\r\nBAZ\r\n:2\r\n$3\r\nBoz\r\n:3\r\n$3\r\nBiz\r\n:4\r\n$5\r\nOther\r\n:5\r\n",
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
				{
					label:     "map",
					msg:       "%5\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
				{
					label:     "map-bulk-fields",
					msg:       "%5\r\n$3\r\nFoo\r\n:1\r\n$3\r\nBAZ\r\n:2\r\n$3\r\nBoz\r\n:3\r\n$3\r\nBiz\r\n:4\r\n$5\r\nOther\r\n:5\r\n",
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
				{
					label:     "map-exact",
					msg:       "%4\r\n+Foo\r\n:1\r\n+BAZ\r\n$1\r\n2\r\n+Boz\r\n:3\r\n+Biz\r\n$1\r\n4\r\n",
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
				{
					msgs: []string{
						"*?\r\n",
						"+Foo\r\n", ":1\r\n",
						"+BAZ\r\n", ":2\r\n",
						"+Boz\r\n", ":3\r\n",
						"+Biz\r\n", ":4\r\n",
						".\r\n",
					},
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
				{
					msgs: []string{
						"%?\r\n",
						"+Foo\r\n", ":1\r\n",
						"+BAZ\r\n", ":2\r\n",
						"+Boz\r\n", ":3\r\n",
						"+Biz\r\n", ":4\r\n",
						".\r\n",
					},
					flattened: []string{"Foo", "1", "BAZ", "2", "Boz", "3", "Biz", "4"},
				},
			},
			mkCases: func(in in) []kase {
				isExact := in.label == "map-exact"
				return []kase{
					{r: isExact, ie: ie{testStructA{}, testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructA{}, &testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{testStructA{TestStructInner{bar: 6}, []byte("foo")}, testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructA{TestStructInner{bar: 6}, []byte("foo")}, &testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{testStructB{}, testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructB{}, &testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{testStructB{&TestStructInner{bar: 6}, []byte("foo")}, testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{r: isExact, ie: ie{&testStructB{&TestStructInner{bar: 6}, []byte("foo")}, &testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
				}
			},
		},
		{
			descr: "empty streamed string",
			ins: []in{
				{
					msgs:      []string{"$?\r\n", ";0\r\n"},
					flattened: []string{""},
					mkCases: func() []kase {
						return []kase{
							{ie: ie{nil, []byte{}}},
							{r: true, ie: ie{streamedStrRW{}, streamedStrRW{}}},
							{r: true, ie: ie{
								msgSeries{&BlobString{}, &StreamedStringChunk{}},
								msgSeries{&BlobString{StreamedStringHeader: true}, &StreamedStringChunk{S: ""}},
							}},
							{r: true, ie: ie{
								msgSeries{&BlobString{S: "foo"}, &StreamedStringChunk{S: "foo"}},
								msgSeries{&BlobString{StreamedStringHeader: true}, &StreamedStringChunk{S: ""}},
							}},
							{r: true, ie: ie{
								msgSeries{&BlobStringBytes{}, &StreamedStringChunkBytes{}},
								msgSeries{&BlobStringBytes{StreamedStringHeader: true}, &StreamedStringChunkBytes{B: []byte{}}},
							}},
							{r: true, ie: ie{
								msgSeries{&BlobStringBytes{B: []byte("foo")}, &StreamedStringChunkBytes{B: []byte("foo")}},
								msgSeries{&BlobStringBytes{StreamedStringHeader: true}, &StreamedStringChunkBytes{B: []byte{}}},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				return setReversable(false, strCases(in, ""))
			},
		},
		{
			descr: "streamed string",
			ins: []in{
				{
					msgs:      []string{"$?\r\n", ";4\r\nohey\r\n", ";0\r\n"},
					flattened: []string{"ohey"},
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{
								streamedStrRW{},
								streamedStrRW{[]byte("ohey")},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobString{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
								},
								msgSeries{
									&BlobString{StreamedStringHeader: true},
									&StreamedStringChunk{S: "ohey"},
									&StreamedStringChunk{S: ""},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobStringBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
								},
								msgSeries{
									&BlobStringBytes{StreamedStringHeader: true},
									&StreamedStringChunkBytes{B: []byte("ohey")},
									&StreamedStringChunkBytes{B: []byte{}},
								},
							}},
						}
					},
				},
				{
					msgs:      []string{"$?\r\n", ";2\r\noh\r\n", ";2\r\ney\r\n", ";0\r\n"},
					flattened: []string{"ohey"},
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{
								streamedStrRW{},
								streamedStrRW{[]byte("oh"), []byte("ey")},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobString{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
								},
								msgSeries{
									&BlobString{StreamedStringHeader: true},
									&StreamedStringChunk{S: "oh"},
									&StreamedStringChunk{S: "ey"},
									&StreamedStringChunk{S: ""},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobStringBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
								},
								msgSeries{
									&BlobStringBytes{StreamedStringHeader: true},
									&StreamedStringChunkBytes{B: []byte("oh")},
									&StreamedStringChunkBytes{B: []byte("ey")},
									&StreamedStringChunkBytes{B: []byte{}},
								},
							}},
						}
					},
				},
				{
					msgs: []string{
						"$?\r\n", ";1\r\no\r\n", ";1\r\nh\r\n", ";2\r\ney\r\n", ";0\r\n",
					},
					flattened: []string{"ohey"},
					mkCases: func() []kase {
						return []kase{
							{r: true, ie: ie{
								streamedStrRW{},
								streamedStrRW{[]byte("o"), []byte("h"), []byte("ey")},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobString{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
									&StreamedStringChunk{},
								},
								msgSeries{
									&BlobString{StreamedStringHeader: true},
									&StreamedStringChunk{S: "o"},
									&StreamedStringChunk{S: "h"},
									&StreamedStringChunk{S: "ey"},
									&StreamedStringChunk{S: ""},
								},
							}},
							{r: true, ie: ie{
								msgSeries{
									&BlobStringBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
									&StreamedStringChunkBytes{},
								},
								msgSeries{
									&BlobStringBytes{StreamedStringHeader: true},
									&StreamedStringChunkBytes{B: []byte("o")},
									&StreamedStringChunkBytes{B: []byte("h")},
									&StreamedStringChunkBytes{B: []byte("ey")},
									&StreamedStringChunkBytes{B: []byte{}},
								},
							}},
						}
					},
				},
			},
			mkCases: func(in in) []kase {
				cases := setReversable(false, strCases(in, "ohey"))
				cases = append(cases, []kase{
					{ie: ie{nil, []byte("ohey")}},
				}...)
				return cases
			},
		},
	}

	opts := resp.NewOpts()
	opts.Deterministic = true
	for _, umt := range unmarshalMarshalTests {
		t.Run(umt.descr, func(t *testing.T) {
			for i, in := range umt.ins {
				inMsg := in.mkMsg()

				assertMarshals := func(t *testing.T, exp string, i interface{}) {
					t.Logf("%#v -> %q", i, inMsg)
					buf := new(bytes.Buffer)
					assert.NoError(t, Marshal(buf, i, opts))
					assert.Equal(t, exp, buf.String())
				}

				assertMarshalsFlatStr := func(t *testing.T, exp []string, i interface{}) {
					t.Logf("%#v (flattened to strings) -> %q", i, in.flattened)
					outStrs, err := Flatten(i, opts)
					assert.NoError(t, err)
					if len(exp) == 0 {
						assert.Empty(t, outStrs)
					} else {
						assert.Equal(t, exp, outStrs)
					}
				}

				if umt.shouldErr != nil {
					buf := bytes.NewBufferString(inMsg)
					br := bufio.NewReader(buf)
					err := Unmarshal(br, nil, opts)
					assert.Equal(t, umt.shouldErr, err)
					assert.Zero(t, br.Buffered())
					assert.Empty(t, buf.Bytes())

					var errConnUsable resp.ErrConnUsable
					assert.True(t, errors.As(err, &errConnUsable))
					assertMarshals(t, inMsg, errConnUsable.Err)
					continue
				}

				t.Run("discard", func(t *testing.T) {
					buf := bytes.NewBufferString(inMsg)
					br := bufio.NewReader(buf)
					err := Unmarshal(br, nil, opts)
					assert.NoError(t, err)
					assert.Zero(t, br.Buffered())
					assert.Empty(t, buf.Bytes())
				})

				testName := in.label
				if testName == "" {
					testName = fmt.Sprintf("in%d", i)
				}

				t.Run(testName, func(t *testing.T) {
					t.Run("raw message", func(t *testing.T) {
						buf := bytes.NewBufferString(inMsg)
						br := bufio.NewReader(buf)

						if len(in.msgs) == 0 {
							in.msgs = []string{in.msg}
						}

						var rawMsgs []string
						for range in.msgs {
							var rm RawMessage
							assert.NoError(t, rm.UnmarshalRESP(br, opts))
							rawMsgs = append(rawMsgs, string(rm))
						}
						assert.Equal(t, in.msgs, rawMsgs)
						assert.Zero(t, br.Buffered())
						assert.Empty(t, buf.Bytes(), "%q", buf.String())
					})

					run := func(withAttr, marshalAsFlatStr bool) func(t *testing.T) {
						return func(t *testing.T) {
							for _, kase := range umt.cases(in) {
								t.Run(kase.label, func(t *testing.T) {
									t.Logf("%q -> %#v", inMsg, kase.ie[0])
									buf := new(bytes.Buffer)
									br := bufio.NewReader(buf)

									// test unmarshaling
									if withAttr {
										_ = AttributeHeader{NumPairs: 2}.MarshalRESP(buf, opts)
										_ = SimpleString{S: "foo"}.MarshalRESP(buf, opts)
										_ = SimpleString{S: "1"}.MarshalRESP(buf, opts)
										_ = SimpleString{S: "bar"}.MarshalRESP(buf, opts)
										_ = SimpleString{S: "2"}.MarshalRESP(buf, opts)
									}
									buf.WriteString(inMsg)

									var intoPtrVal reflect.Value
									if kase.ie[0] == nil {
										intoPtrVal = reflect.ValueOf(&kase.ie[0])
									} else {
										intoOrigVal := reflect.ValueOf(kase.ie[0])
										intoPtrVal = reflect.New(intoOrigVal.Type())
										intoPtrVal.Elem().Set(intoOrigVal)
									}

									err := Unmarshal(br, intoPtrVal.Interface(), opts)
									assert.NoError(t, err)

									into := intoPtrVal.Elem().Interface()
									exp := kase.ie[1]
									switch exp := exp.(type) {
									case *big.Int:
										assert.Zero(t, exp.Cmp(into.(*big.Int)))
									case *big.Float:
										assert.Zero(t, exp.Cmp(into.(*big.Float)))
									case BigNumber:
										assert.Zero(t, exp.I.Cmp(into.(BigNumber).I))
									default:
										assert.Equal(t, exp, into)
									}
									assert.Empty(t, buf.Bytes())
									assert.Zero(t, br.Buffered())

									if kase.r {
										if _, marshaler := exp.(resp.Marshaler); !marshaler && marshalAsFlatStr {
											flattened := in.flattened
											if kase.flattenedEmpty {
												flattened = nil
											}
											assertMarshalsFlatStr(t, flattened, exp)
										} else {
											assertMarshals(t, inMsg, exp)
										}
									}
								})
							}
						}
					}

					t.Run("without attr/marshal", run(false, false))
					t.Run("with attr/marshal", run(true, false))
					t.Run("without attr/marshalFlatStr", run(false, true))
					t.Run("with attr/marshalFlatStr", run(true, true))
				})
			}
		})
	}
}
