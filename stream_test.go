package radix

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"math"
	"strings"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamEntryID(t *T) {
	t.Run("Before", func(t *T) {
		for _, test := range []struct {
			S StreamEntryID
			O StreamEntryID
		}{
			{
				S: StreamEntryID{Time: 0, Seq: 0},
				O: StreamEntryID{Time: 0, Seq: 1},
			},
			{
				S: StreamEntryID{Time: 0, Seq: 0},
				O: StreamEntryID{Time: 1, Seq: 0},
			},
			{
				S: StreamEntryID{Time: 1, Seq: 0},
				O: StreamEntryID{Time: 1, Seq: 1},
			},
			{
				S: StreamEntryID{Time: 1, Seq: math.MaxUint64},
				O: StreamEntryID{Time: 2, Seq: 0},
			},
		} {
			assert.Truef(t, test.S.Before(test.O), "%q is not before %q", test.S, test.O)
			assert.Falsef(t, test.O.Before(test.S), "%q is not before %q (reverse check)", test.S, test.O)
		}
	})

	t.Run("Prev", func(t *T) {
		for _, test := range []struct {
			S StreamEntryID
			E StreamEntryID
		}{
			{
				S: StreamEntryID{Time: 0, Seq: 0},
				E: StreamEntryID{Time: 0, Seq: 0},
			},
			{
				S: StreamEntryID{Time: 0, Seq: 1},
				E: StreamEntryID{Time: 0, Seq: 0},
			},
			{
				S: StreamEntryID{Time: 1, Seq: 0},
				E: StreamEntryID{Time: 0, Seq: math.MaxUint64},
			},
			{
				S: StreamEntryID{Time: 1, Seq: 1},
				E: StreamEntryID{Time: 1, Seq: 0},
			},
			{
				S: StreamEntryID{Time: 1, Seq: math.MaxUint64},
				E: StreamEntryID{Time: 1, Seq: math.MaxUint64 - 1},
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: math.MaxUint64},
				E: StreamEntryID{Time: math.MaxUint64, Seq: math.MaxUint64 - 1},
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: 0},
				E: StreamEntryID{Time: math.MaxUint64 - 1, Seq: math.MaxUint64},
			},
		} {
			assert.Equal(t, test.E, test.S.Prev())
		}
	})

	t.Run("Next", func(t *T) {
		for _, test := range []struct {
			S StreamEntryID
			E StreamEntryID
		}{
			{
				S: StreamEntryID{Time: 0, Seq: 0},
				E: StreamEntryID{Time: 0, Seq: 1},
			},
			{
				S: StreamEntryID{Time: 0, Seq: 1},
				E: StreamEntryID{Time: 0, Seq: 2},
			},
			{
				S: StreamEntryID{Time: 1, Seq: 0},
				E: StreamEntryID{Time: 1, Seq: 1},
			},
			{
				S: StreamEntryID{Time: 1, Seq: 1},
				E: StreamEntryID{Time: 1, Seq: 2},
			},
			{
				S: StreamEntryID{Time: 1, Seq: math.MaxUint64 - 1},
				E: StreamEntryID{Time: 1, Seq: math.MaxUint64},
			},
			{
				S: StreamEntryID{Time: 1, Seq: math.MaxUint64},
				E: StreamEntryID{Time: 2, Seq: 0},
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: math.MaxUint64},
				E: StreamEntryID{Time: math.MaxUint64, Seq: math.MaxUint64},
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: 0},
				E: StreamEntryID{Time: math.MaxUint64, Seq: 1},
			},
		} {
			assert.Equal(t, test.E, test.S.Next())
		}
	})

	t.Run("Stream", func(t *T) {
		for _, test := range []struct {
			S StreamEntryID
			E string
		}{
			{
				S: StreamEntryID{Time: 0, Seq: 0},
				E: "0-0",
			},
			{
				S: StreamEntryID{Time: 0, Seq: 1},
				E: "0-1",
			},
			{
				S: StreamEntryID{Time: 0, Seq: math.MaxUint64},
				E: "0-18446744073709551615",
			},
			{
				S: StreamEntryID{Time: 1, Seq: 0},
				E: "1-0",
			},
			{
				S: StreamEntryID{Time: 1, Seq: 1},
				E: "1-1",
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: 0},
				E: "18446744073709551615-0",
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: 1000},
				E: "18446744073709551615-1000",
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: math.MaxUint64},
				E: "18446744073709551615-18446744073709551615",
			},
		} {
			assert.Equal(t, test.E, test.S.String())
		}
	})

	t.Run("MarshalRESP", func(t *T) {
		for _, test := range []struct {
			S StreamEntryID
			E string
		}{
			{
				S: StreamEntryID{Time: 0, Seq: 0},
				E: "$3\r\n0-0\r\n",
			},
			{
				S: StreamEntryID{Time: 0, Seq: 1},
				E: "$3\r\n0-1\r\n",
			},
			{
				S: StreamEntryID{Time: 1, Seq: 0},
				E: "$3\r\n1-0\r\n",
			},
			{
				S: StreamEntryID{Time: 1, Seq: 1},
				E: "$3\r\n1-1\r\n",
			},
			{
				S: StreamEntryID{Time: 1234, Seq: 5678},
				E: "$9\r\n1234-5678\r\n",
			},
			{
				S: StreamEntryID{Time: 0, Seq: math.MaxUint64},
				E: "$22\r\n0-18446744073709551615\r\n",
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: 0},
				E: "$22\r\n18446744073709551615-0\r\n",
			},
			{
				S: StreamEntryID{Time: math.MaxUint64, Seq: math.MaxUint64},
				E: "$41\r\n18446744073709551615-18446744073709551615\r\n",
			},
		} {
			var buf bytes.Buffer
			require.Nil(t, test.S.MarshalRESP(&buf))
			assert.Equal(t, test.E, buf.String())
		}
	})

	t.Run("UnmarshalRESP", func(t *T) {
		for _, test := range []struct {
			In  string
			E   StreamEntryID
			Err string
		}{
			{
				In:  "",
				Err: "EOF",
			},
			{
				In:  "$-1\r\n\r\n",
				Err: "invalid stream entry id",
			},
			{
				In:  "$0\r\n\r\n",
				Err: "invalid stream entry id",
			},
			{
				In:  "$4\r\n\r\n",
				Err: "unexpected EOF",
			},
			{
				In:  "$1\r\n0\r\n",
				Err: "invalid stream entry id",
			},
			{
				In:  "$2\r\n0-\r\n",
				Err: "invalid stream entry id",
			},
			{
				In:  "$2\r\n-0\r\n",
				Err: "invalid stream entry id",
			},
			{
				In:  "$4\r\n0--0\r\n",
				Err: "invalid stream entry id",
			},
			{
				In:  "$4\r\n0-+0\r\n",
				Err: "invalid stream entry id",
			},
			{
				In: "$3\r\n0-0\r\n",
				E:  StreamEntryID{Time: 0, Seq: 0},
			},
			{
				In: "$3\r\n0-1\r\n",
				E:  StreamEntryID{Time: 0, Seq: 1},
			},
			{
				In: "$3\r\n1-0\r\n",
				E:  StreamEntryID{Time: 1, Seq: 0},
			},
			{
				In: "$3\r\n1-1\r\n",
				E:  StreamEntryID{Time: 1, Seq: 1},
			},
			{
				In: "$5\r\n20-20\r\n",
				E:  StreamEntryID{Time: 20, Seq: 20},
			},
			{
				In: "$41\r\n18446744073709551615-18446744073709551615\r\n",
				E:  StreamEntryID{Time: 18446744073709551615, Seq: 18446744073709551615},
			},
		} {
			br := bufio.NewReader(strings.NewReader(test.In))

			var s StreamEntryID
			err := s.UnmarshalRESP(br)

			if test.Err == "" {
				assert.NoErrorf(t, err, "failed to unmarshal %q", test.In)
			} else {
				assert.EqualError(t, err, test.Err)
			}

			assert.Equal(t, test.E, s)
		}
	})
}

var benchErr error
var benchString string

func BenchmarkStreamEntryID(b *B) {
	b.Run("String", func(b *B) {
		s := &StreamEntryID{Time: 1234, Seq: 5678}

		for i := 0; i < b.N; i++ {
			benchString = s.String()
		}
	})

	b.Run("MarshalRESP", func(b *B) {
		s := &StreamEntryID{Time: 1234, Seq: 5678}

		for i := 0; i < b.N; i++ {
			benchErr = s.MarshalRESP(ioutil.Discard)
		}
	})

	b.Run("UnmarshalRESP", func(b *B) {
		s := &StreamEntryID{Time: 1234, Seq: 5678}

		buf := strings.NewReader("")
		br := bufio.NewReader(buf)

		for i := 0; i < b.N; i++ {
			buf.Reset("$9\r\n1234-5678\r\n")
			br.Reset(buf)

			benchErr = s.UnmarshalRESP(br)
		}
	})
}

func TestStreamEntry(t *T) {
	c := dial()
	defer c.Close()

	stream := randStr()

	var id1, id2 string
	require.NoError(t, c.Do(Cmd(&id1, "XADD", stream, "*", "hello", "world", "foo", "bar")))
	require.NoError(t, c.Do(Cmd(&id2, "XADD", stream, "*", "hello", "bar")))

	var entries []StreamEntry
	assert.NoError(t, c.Do(Cmd(&entries, "XRANGE", stream, "-", "+")))
	assert.Len(t, entries, 2, "got wrong number of stream entries")

	assert.Equal(t, id1, entries[0].ID.String(), "parsed ID differs from ID returned by XADD")
	assert.Equal(t, map[string]string{"hello": "world", "foo": "bar"}, entries[0].Fields)

	assert.Equal(t, id2, entries[1].ID.String(), "parsed ID differs from ID returned by XADD")
	assert.Equal(t, map[string]string{"hello": "bar"}, entries[1].Fields)

	assert.True(t, entries[0].ID.Before(entries[1].ID))
}

func BenchmarkStreamEntry(b *B) {
	c := dial()
	defer c.Close()

	stream := randStr()
	require.NoError(b, c.Do(Cmd(nil, "XADD", stream, "*", "hello", "world", "foo", "bar")))

	b.ResetTimer()

	// preallocate so we don't measure the slice allocation cost
	entries := make([]StreamEntry, 0, 1)

	for i := 0; i < b.N; i++ {
		entries = entries[:0]
		benchErr = c.Do(Cmd(&entries, "XRANGE", stream, "-", "+"))
	}
}
