package radix

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	. "testing"
	"time"

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

func TestStreamReader(t *T) {
	t.Run("Group", func(t *T) {
		t.Run("Empty", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: {Time: 0, Seq: 0},
					stream2: {Time: 0, Seq: 0},
				},
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			})

			addStreamGroup(t, c, stream1, group, "0-0")
			addStreamGroup(t, c, stream2, group, "0-0")

			assertNoStreamReaderEntries(t, r)
		})

		t.Run("NoGroup", func(t *T) {
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: {Time: 0, Seq: 0},
					stream2: {Time: 0, Seq: 0},
				},
				Group:   randStr(),
				NoBlock: true,
			})

			_, _, ok := r.Next()
			assert.False(t, ok)
			err := r.Err()
			assert.Error(t, err)
		})

		t.Run("EOS", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: nil,
					stream2: nil,
				},
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			})

			addStreamGroup(t, c, stream1, group, "0-0")
			addStreamGroup(t, c, stream2, group, "0-0")

			id1 := addStreamEntry(t, c, stream1)
			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream1: {id1}})

			id2 := addStreamEntry(t, c, stream2)
			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream2: {id2}})

			assertNoStreamReaderEntries(t, r)
			assertConsumer(t, c, stream1, group, consumer, 1)
			assertConsumer(t, c, stream2, group, consumer, 1)
		})

		t.Run("Count", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: nil,
					stream2: nil,
				},
				Group:    group,
				Consumer: consumer,
				Count:    2,
				NoBlock:  true,
			})

			addStreamGroup(t, c, stream1, group, "0-0")
			addStreamGroup(t, c, stream2, group, "0-0")

			ids1 := addNStreamEntries(t, c, stream1, 5)
			ids2 := addNStreamEntries(t, c, stream2, 3)

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: ids1[:2],
				stream2: ids2[:2],
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: ids1[2:4],
				stream2: ids2[2:],
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: ids1[4:],
			})

			assertNoStreamReaderEntries(t, r)
			assertConsumer(t, c, stream1, group, consumer, 5)
			assertConsumer(t, c, stream2, group, consumer, 3)
		})

		t.Run("WrongType", func(t *T) {
			c := dial()
			defer c.Close()

			stream1 := randStr()

			require.NoError(t, c.Do(Cmd(nil, "SET", stream1, "1")))

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: nil,
				},
				Group:    randStr(),
				Consumer: randStr(),
			})

			_, _, ok := r.Next()
			assert.False(t, ok)
			err := r.Err()
			assert.Error(t, err)

			_, _, ok = r.Next()
			assert.False(t, ok)
			assert.Equal(t, err, r.Err())
		})

		t.Run("Blocking", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
				Group:    group,
				Consumer: consumer,
				Block:    0,
				Count:    10,
			})

			addStreamGroup(t, c, stream, group, "0-0")

			for i := 1; i <= 2; i++ {
				idChan := make(chan StreamEntryID, 1)
				time.AfterFunc(5*time.Millisecond, func() {
					c := dial()
					defer c.Close()
					idChan <- addStreamEntry(t, c, stream)
				})

				_, entries, ok := r.Next()
				id := <-idChan
				assert.True(t, ok)
				assert.Len(t, entries, 1)
				assert.Equal(t, id, entries[0].ID)
				assert.NoError(t, r.Err())

				assertConsumer(t, c, stream, group, consumer, i)
			}
		})

		t.Run("Timeout", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
				Group:    group,
				Consumer: consumer,
				Block:    500 * time.Millisecond,
			})

			addStreamGroup(t, c, stream, group, "0-0")

			assertNoStreamReaderEntries(t, r)

			idChan := make(chan StreamEntryID, 1)
			time.AfterFunc(100*time.Millisecond, func() {
				c := dial()
				defer c.Close()

				idChan <- addStreamEntry(t, c, stream)
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream: {<-idChan}})
			assertConsumer(t, c, stream, group, consumer, 1)
		})

		t.Run("NoAck", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
				NoAck:    true,
			})

			addStreamGroup(t, c, stream, group, "0-0")

			id := addStreamEntry(t, c, stream)
			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream: {id}})
			assertNoStreamReaderEntries(t, r)
			assertConsumer(t, c, stream, group, consumer, 0)
		})

		t.Run("Unacknowledged", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r1 := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			})

			addStreamGroup(t, c, stream, group, "0-0")

			assertNoStreamReaderEntries(t, r1)
			id1 := addStreamEntry(t, c, stream)
			assertStreamReaderEntries(t, r1, map[string][]StreamEntryID{stream: {id1}})
			id2 := addStreamEntry(t, c, stream)

			r2 := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: {Time: 0, Seq: 0},
				},
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			})

			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id1}})
			assertConsumer(t, c, stream, group, consumer, 1)

			assertStreamReaderEntries(t, r1, map[string][]StreamEntryID{stream: {id2}})
			assertNoStreamReaderEntries(t, r1)

			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id2}})
			assertNoStreamReaderEntries(t, r2)
			assertConsumer(t, c, stream, group, consumer, 2)
		})
	})

	t.Run("NoGroup", func(t *T) {
		t.Run("Empty", func(t *T) {
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: {Time: 0, Seq: 0},
					stream2: {Time: 0, Seq: 0},
				},
				NoBlock: true,
			})

			assertNoStreamReaderEntries(t, r)
		})

		t.Run("EOS", func(t *T) {
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: {Time: 0, Seq: 0},
					stream2: {Time: 0, Seq: 0},
				},
				NoBlock: true,
			})

			id1 := addStreamEntry(t, c, stream1)
			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream1: {id1}})

			id2 := addStreamEntry(t, c, stream2)
			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream2: {id2}})

			assertNoStreamReaderEntries(t, r)
		})

		t.Run("Count", func(t *T) {
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream1: {Time: 0, Seq: 0},
					stream2: {Time: 0, Seq: 0},
				},
				Count:   2,
				NoBlock: true,
			})

			ids1 := addNStreamEntries(t, c, stream1, 5)
			ids2 := addNStreamEntries(t, c, stream2, 3)

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: ids1[:2],
				stream2: ids2[:2],
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: ids1[2:4],
				stream2: ids2[2:],
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: ids1[4:],
			})

			assertNoStreamReaderEntries(t, r)
		})

		t.Run("WrongType", func(t *T) {
			c := dial()
			defer c.Close()

			stream := randStr()

			require.NoError(t, c.Do(Cmd(nil, "SET", stream, "1")))

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
			})

			_, _, ok := r.Next()
			assert.False(t, ok)
			err := r.Err()
			assert.Error(t, err)

			_, _, ok = r.Next()
			assert.False(t, ok)
			assert.Equal(t, err, r.Err())
		})

		t.Run("Blocking", func(t *T) {
			c := dial()
			defer c.Close()

			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
				Block: 0,
				Count: 10,
			})

			for i := 0; i < 2; i++ {
				idChan := make(chan StreamEntryID, 1)
				time.AfterFunc(5*time.Millisecond, func() {
					c := dial()
					defer c.Close()
					idChan <- addStreamEntry(t, c, stream)
				})

				_, entries, ok := r.Next()
				id := <-idChan
				assert.True(t, ok)
				assert.Len(t, entries, 1)
				assert.Equal(t, id, entries[0].ID)
				assert.NoError(t, r.Err())
			}
		})

		t.Run("Timeout", func(t *T) {
			c := dial()
			defer c.Close()

			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: {Time: 0, Seq: 0},
				},
				Block: 500 * time.Millisecond,
			})

			assertNoStreamReaderEntries(t, r)

			idChan := make(chan StreamEntryID, 1)
			time.AfterFunc(100*time.Millisecond, func() {
				c := dial()
				defer c.Close()

				idChan <- addStreamEntry(t, c, stream)
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{stream: {<-idChan}})
		})
	})
}

func BenchmarkStreamReader(b *B) {
	c := dial()
	defer c.Close()

	stream := randStr()
	streams := map[string]*StreamEntryID{
		stream: {Time: 0, Seq: 0},
	}

	addNStreamEntries(b, c, stream, 32)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := NewStreamReader(c, StreamReaderOpts{
			Streams: streams,
			NoBlock: true,
		})

		for _, entries, ok := r.Next(); ok && len(entries) > 0; _, entries, ok = r.Next() {
			benchErr = r.Err()
		}
	}
}

func addStreamEntry(tb TB, c Client, stream string) StreamEntryID {
	tb.Helper()
	var id StreamEntryID
	require.NoError(tb, c.Do(Cmd(&id, "XADD", stream, "*", randStr(), randStr())))
	return id
}

func addNStreamEntries(tb TB, c Client, stream string, n int) []StreamEntryID {
	tb.Helper()
	ids := make([]StreamEntryID, n)
	for i := 0; i < n; i++ {
		ids[i] = addStreamEntry(tb, c, stream)
	}
	return ids
}

func assertNoStreamReaderEntries(tb TB, r StreamReader) {
	tb.Helper()
	stream, entries, ok := r.Next()
	assert.Empty(tb, stream)
	assert.Empty(tb, entries)
	assert.True(tb, ok)
}

func assertStreamReaderEntries(tb TB, r StreamReader, expected map[string][]StreamEntryID) {
	tb.Helper()

	for len(expected) > 0 {
		stream, entries, ok := r.Next()
		if !ok {
			break
		}
		if _, ok := expected[stream]; !ok {
			assert.Fail(tb, "unexpected stream entries", "for %s", stream)
			break
		}

		got := make([]StreamEntryID, len(entries))
		for i, e := range entries {
			got[i] = e.ID
		}

		assert.Equal(tb, expected[stream], got)
		delete(expected, stream)
	}

	if len(expected) > 0 {
		assert.Fail(tb, "unexpected end of stream", "expected one of: %v", expected)
	}

	assert.NoError(tb, r.Err())
}

func addStreamGroup(tb TB, c Client, stream, group, id string) {
	tb.Helper()
	require.NoError(tb, c.Do(Cmd(nil, "XGROUP", "CREATE", stream, group, id, "MKSTREAM")))
}

func assertConsumer(tb TB, c Client, stream, group, consumer string, pending int) {
	tb.Helper()
	var cs []map[string]string
	require.NoError(tb, c.Do(Cmd(&cs, "XINFO", "CONSUMERS", stream, group)))

	for _, c := range cs {
		if c["name"] != consumer {
			continue
		}

		got, _ := strconv.Atoi(c["pending"])
		assert.Equal(tb, pending, got, "wrong number of pending messages")
		return
	}

	assert.Failf(tb, "pending messages assertion failed", "consumer %s not in group %s for stream %s", consumer, group, stream)
}
