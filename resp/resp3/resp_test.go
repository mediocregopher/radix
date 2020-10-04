package resp3

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"errors"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeekAndAssertPrefix(t *testing.T) {
	type test struct {
		in     []byte
		prefix Prefix
		exp    error
	}

	tests := []test{
		{[]byte(":5\r\n"), NumberPrefix, nil},
		{[]byte(":5\r\n"), SimpleStringPrefix, resp.ErrConnUsable{
			Err: errUnexpectedPrefix{
				Prefix: NumberPrefix, ExpectedPrefix: SimpleStringPrefix,
			},
		}},
		{[]byte("-foo\r\n"), SimpleErrorPrefix, nil},
		{[]byte("-foo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: SimpleError{
			S: "foo",
		}}},
		{[]byte("!3\r\nfoo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: BlobError{
			B: []byte("foo"),
		}}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			br := bufio.NewReader(bytes.NewReader(test.in))
			err := peekAndAssertPrefix(br, test.prefix, false)

			assert.IsType(t, test.exp, err)
			if expUsable, ok := test.exp.(resp.ErrConnUsable); ok {
				usable, _ := err.(resp.ErrConnUsable)
				assert.IsType(t, expUsable.Err, usable.Err)
			}
			if test.exp != nil {
				assert.Equal(t, test.exp.Error(), err.Error())
			}
		})
	}
}

func TestAnyConsumedOnErr(t *testing.T) {
	type foo struct {
		Foo int
		Bar int
	}

	type test struct {
		in   resp.Marshaler
		into interface{}
	}

	type unknownType string

	tests := []test{
		{Any{I: errors.New("foo")}, new(unknownType)},
		{BlobString{S: "blobStr"}, new(unknownType)},
		{SimpleString{S: "blobStr"}, new(unknownType)},
		{Number{N: 1}, new(unknownType)},
		{Any{I: []string{"one", "2", "three"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "3", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four", "five"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "three", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "3", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []interface{}{1, 2, "Bar", "two"}}, new(foo)},
		{Any{I: []string{"Foo", "1", "Bar", "two"}}, new(foo)},
		{Any{I: [][]string{{"one", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"3", "four"}}}, new([][]int)},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			require.Nil(t, test.in.MarshalRESP(buf))
			require.Nil(t, SimpleString{S: "DISCARDED"}.MarshalRESP(buf))
			br := bufio.NewReader(buf)

			err := Any{I: test.into}.UnmarshalRESP(br)
			assert.Error(t, err)
			assert.True(t, errors.As(err, new(resp.ErrConnUsable)))

			var ss SimpleString
			assert.NoError(t, ss.UnmarshalRESP(br))
			assert.Equal(t, "DISCARDED", ss.S)
		})
	}
}

func TestRawMessage(t *testing.T) {
	type test struct {
		msg                                  string
		expNull, expEmpty, expStreamedHeader bool
	}

	tests := []test{
		{
			msg: "",
		},
		{
			msg: "$3\r\nfoo\r\n",
		},
		{
			msg:     "_\r\n",
			expNull: true,
		},
		{
			msg:     "*-1\r\n",
			expNull: true,
		},
		{
			msg:     "$-1\r\n",
			expNull: true,
		},
		{
			msg:      "*0\r\n",
			expEmpty: true,
		},
		{
			msg: "*1\r\n:1\r\n",
		},
		{
			msg:      "~0\r\n",
			expEmpty: true,
		},
		{
			msg: "~1\r\n:1\r\n",
		},
		{
			msg:      "%0\r\n",
			expEmpty: true,
		},
		{
			msg: "%1\r\n:1\r\n:2\r\n",
		},
		{
			msg:      ">0\r\n",
			expEmpty: true,
		},
		{
			msg: ">1\r\n:1\r\n",
		},
		{
			msg:      "|0\r\n",
			expEmpty: true,
		},
		{
			msg: "|1\r\n:1\r\n:2\r\n",
		},
		{
			msg:               "$?\r\n",
			expStreamedHeader: true,
		},
		{
			msg:               "*?\r\n",
			expStreamedHeader: true,
		},
		{
			msg:               "~?\r\n",
			expStreamedHeader: true,
		},
		{
			msg:               "%?\r\n",
			expStreamedHeader: true,
		},
		{
			msg: ".\r\n",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Logf("%q", test.msg)
			rm := RawMessage(test.msg)
			assert.Equal(t, test.expNull, rm.IsNull(), "IsNull")
			assert.Equal(t, test.expEmpty, rm.IsEmpty(), "IsEmpty")
			assert.Equal(t, test.expStreamedHeader, rm.IsStreamedHeader(), "IsStreamedHeader")
		})
	}
}

func Example_streamedAggregatedType() {
	buf := new(bytes.Buffer)

	// First write a streamed array to the buffer. The array will have 3 number
	// elements.
	(ArrayHeader{StreamedArrayHeader: true}).MarshalRESP(buf)
	(Number{N: 1}).MarshalRESP(buf)
	(Number{N: 2}).MarshalRESP(buf)
	(Number{N: 3}).MarshalRESP(buf)
	(StreamedAggregatedTypeEnd{}).MarshalRESP(buf)

	// Now create a reader which will read from the buffer, and use it to read
	// the streamed array.
	br := bufio.NewReader(buf)

	// The type of the next message can be checked by peeking at the next byte.
	if prefixB, _ := br.Peek(1); Prefix(prefixB[0]) != ArrayHeaderPrefix {
		panic("expected array header")
	}

	var head ArrayHeader
	head.UnmarshalRESP(br)
	if !head.StreamedArrayHeader {
		panic("expected streamed array header")
	}
	fmt.Println("streamed array begun")

	for {
		var el Number
		aggEl := StreamedAggregatedElement{Unmarshaler: &el}
		aggEl.UnmarshalRESP(br)
		if aggEl.End {
			fmt.Println("streamed array ended")
			return
		}
		fmt.Printf("read element with value %d\n", el.N)
	}

	// Output: streamed array begun
	// read element with value 1
	// read element with value 2
	// read element with value 3
	// streamed array ended
}
