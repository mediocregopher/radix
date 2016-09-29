package radix

import (
	"bytes"
	"errors"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCast(t *T) {

	assertTypeBody := func(r Resp, expTyp riType, expBody []byte) {
		assert.Equal(t, expTyp, r.riType)
		assert.Equal(t, expBody, r.body)
	}

	readTest := func(in string, expTyp riType, expBody []byte) Resp {
		buf := bytes.NewBufferString(in)
		r := NewRespReader(buf).Read()
		assertTypeBody(r, expTyp, expBody)
		return r
	}

	doesErr := struct{}{}
	type castExp struct {
		onNil     interface{}
		onBytes   interface{}
		onInt64   interface{}
		onFloat64 interface{}
		onArray   interface{}
		err       string
	}

	castTestSingle := func(exp, out interface{}, err error) {
		if exp == doesErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, exp, out)
		}
	}

	castTest := func(r Resp, ce castExp) {
		var out interface{}
		var err error

		out, err = r.Nil()
		castTestSingle(ce.onNil, out, err)
		out, err = r.Bytes()
		castTestSingle(ce.onBytes, out, err)
		out, err = r.Int64()
		castTestSingle(ce.onInt64, out, err)
		out, err = r.Float64()
		castTestSingle(ce.onFloat64, out, err)
		out, err = r.Array()
		castTestSingle(ce.onArray, out, err)

		if ce.err != "" {
			assert.Equal(t, ce.err, r.Err.Error())
		} else {
			assert.Nil(t, r.Err)
		}
	}

	var r Resp

	// Simple string
	r = readTest("+ohey\r\n", riSimpleStr, []byte("ohey"))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   []byte("ohey"),
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   doesErr,
		err:       "",
	})

	// Simple string that parses to int
	r = readTest("+10\r\n", riSimpleStr, []byte("10"))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   []byte("10"),
		onInt64:   int64(10),
		onFloat64: float64(10),
		onArray:   doesErr,
		err:       "",
	})

	// Simple string that parses to float
	r = readTest("+10.5\r\n", riSimpleStr, []byte("10.5"))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   []byte("10.5"),
		onInt64:   doesErr,
		onFloat64: float64(10.5),
		onArray:   doesErr,
		err:       "",
	})

	// Empty simple string
	r = readTest("+\r\n", riSimpleStr, []byte(""))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   []byte(""),
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   doesErr,
		err:       "",
	})

	// Error
	r = readTest("-ohey\r\n", riAppErr, []byte("ohey"))
	castTest(r, castExp{
		onNil:     doesErr,
		onBytes:   doesErr,
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   doesErr,
		err:       "ohey",
	})

	// Int
	r = readTest(":1024\r\n", riInt, []byte("1024"))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   doesErr,
		onInt64:   int64(1024),
		onFloat64: float64(1024),
		onArray:   doesErr,
		err:       "",
	})

	// Bulk string
	r = readTest("$3\r\nfoo\r\n", riBulkStr, []byte("foo"))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   []byte("foo"),
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   doesErr,
		err:       "",
	})

	// Empty bulk string
	r = readTest("$0\r\n\r\n", riBulkStr, []byte(""))
	castTest(r, castExp{
		onNil:     false,
		onBytes:   []byte(""),
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   doesErr,
		err:       "",
	})

	// Nil bulk string
	r = readTest("$-1\r\n", riBulkStr, nil)
	castTest(r, castExp{
		onNil:     true,
		onBytes:   []byte(nil),
		onInt64:   int64(0),
		onFloat64: float64(0),
		onArray:   []Resp(nil),
		err:       "",
	})

	// Array
	r = readTest("*2\r\n+foo\r\n+bar\r\n", riArray, nil)
	castTest(r, castExp{
		onNil:     false,
		onBytes:   doesErr,
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   []Resp{NewSimpleString("foo"), NewSimpleString("bar")},
		err:       "",
	})

	assert.Len(t, r.arr, 2)
	assertTypeBody(r.arr[0], riSimpleStr, []byte("foo"))
	assertTypeBody(r.arr[1], riSimpleStr, []byte("bar"))

	l, err := r.List()
	assert.Nil(t, err)
	assert.Equal(t, []string{"foo", "bar"}, l)

	b, err := r.ListBytes()
	assert.Nil(t, err)
	assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar")}, b)

	m, err := r.Map()
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{"foo": "bar"}, m)

	// Empty Array
	r = readTest("*0\r\n", riArray, nil)
	castTest(r, castExp{
		onNil:     false,
		onBytes:   doesErr,
		onInt64:   doesErr,
		onFloat64: doesErr,
		onArray:   []Resp{},
		err:       "",
	})
	assert.Len(t, r.arr, 0)

	// Nil Array
	r = readTest("*-1\r\n", riArray, nil)
	assert.True(t, r.isNil)
	isNil, err := r.Nil()
	assert.Nil(t, err)
	assert.True(t, isNil)

	// Embedded Array
	r = readTest("*3\r\n+foo\r\n+bar\r\n*2\r\n+foo\r\n+bar\r\n", riArray, nil)
	assert.Len(t, r.arr, 3)
	assertTypeBody(r.arr[0], riSimpleStr, []byte("foo"))
	assertTypeBody(r.arr[1], riSimpleStr, []byte("bar"))

	r = r.arr[2]
	assert.Len(t, r.arr, 2)
	assertTypeBody(r.arr[0], riSimpleStr, []byte("foo"))
	assertTypeBody(r.arr[1], riSimpleStr, []byte("bar"))

	// Test that two bulks in a row read correctly
	r = readTest("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", riArray, nil)
	assert.Len(t, r.arr, 2)
	assertTypeBody(r.arr[0], riBulkStr, []byte("foo"))
	assertTypeBody(r.arr[1], riBulkStr, []byte("bar"))
}

type writeTest struct {
	val    interface{}
	expect string
}

var writeTests = []writeTest{
	// Basic types
	{[]byte("OHAI"), "$4\r\nOHAI\r\n"},
	{"OHAI", "$4\r\nOHAI\r\n"},
	{true, ":1\r\n"},
	{false, ":0\r\n"},
	{nil, "$-1\r\n"},
	{80, ":80\r\n"},
	{int64(-80), ":-80\r\n"},
	{uint64(80), ":80\r\n"},
	{float32(0.1234), "$6\r\n0.1234\r\n"},
	{float64(0.1234), "$6\r\n0.1234\r\n"},
	{errors.New("hi"), "-hi\r\n"},

	// Special types (non-Cmd)
	{NewResp(nil), "$-1\r\n"},
	{NewSimpleString("OK"), "+OK\r\n"},

	// Data structure types
	{[]int{1, 2, 3}, "*3\r\n:1\r\n:2\r\n:3\r\n"},
	{map[int]int{1: 2}, "*2\r\n:1\r\n:2\r\n"},
	{
		[]Resp{NewSimpleString("foo"), NewSimpleString("bar")},
		"*2\r\n+foo\r\n+bar\r\n",
	},
	{
		[]interface{}{"wat", map[string]interface{}{
			"foo": 1,
		}},
		"*2\r\n$3\r\nwat\r\n*2\r\n$3\r\nfoo\r\n:1\r\n",
	},
	{
		map[string]interface{}{"foo": true},
		"*2\r\n$3\r\nfoo\r\n:1\r\n",
	},

	// Cmd
	{
		NewCmd("foo", "bar", "baz"),
		"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n",
	},
	{
		NewCmd("foo"),
		"*1\r\n$3\r\nfoo\r\n",
	},
	{
		NewCmd("foo", []string{"bar", "baz"}, 1),
		"*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$1\r\n1\r\n",
	},
	{
		NewCmd("foo", 4.2, map[string]interface{}{"bar": "baz", "buz": nil}),
		"*6\r\n$3\r\nfoo\r\n$3\r\n4.2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$3\r\nbuz\r\n$0\r\n\r\n",
	},
	{
		[]interface{}{"foo", NewCmd("bar", "baz", "buz")},
		"*2\r\n$3\r\nfoo\r\n*3\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$3\r\nbuz\r\n",
	},
}

func TestWrite(t *T) {
	var err error
	buf := bytes.NewBuffer([]byte{})
	for _, test := range writeTests {
		buf.Reset()
		rw := NewRespWriter(buf)
		err = rw.Write(NewResp(test.val))
		assert.Nil(t, err)
		assert.Equal(t, test.expect, buf.String())

		buf.Reset()
		rw = NewRespWriter(buf)
		err = rw.WriteAny(test.val)
		assert.Nil(t, err)
		assert.Equal(t, test.expect, buf.String())
	}
}
