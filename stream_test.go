package radix

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"math"
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
		c := dial()
		defer c.Close()

		stream1, stream2 := randStr(), randStr()
		group1, group2 := randStr(), randStr()

		// MKSTREAM is available since 5.0, but not documented as of Nov 19 2018.
		require.NoError(t, c.Do(Cmd(nil, "XGROUP", "CREATE", stream1, group1, "$", "MKSTREAM")))
		require.NoError(t, c.Do(Cmd(nil, "XGROUP", "CREATE", stream1, group2, "$")))
		require.NoError(t, c.Do(Cmd(nil, "XGROUP", "CREATE", stream2, group1, "$", "MKSTREAM")))
		require.NoError(t, c.Do(Cmd(nil, "XGROUP", "CREATE", stream2, group2, "$")))

		t.Skip("not implemented yet")
		_, _ = stream1, stream2

		// TODO: empty stream (NOGROUP error)
		// TODO: error (WRONGTYPE error)
		// TODO: nothing more to read
		// TODO: count
		// TODO: multiple streams
		// TODO: id (2 stream with IDs, other with no ID)
		// TODO: consumer name
		// TODO: blocking
		// TODO: noack
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
				Block: -1,
			})

			var entries map[string][]StreamEntry
			assert.Zero(t, r.Next(&entries), "got entries for empty streams")
			assert.Empty(t, entries, "got entries for empty streams")

			// behaviour shouldn't change on second call
			assert.Zero(t, r.Next(&entries), "got entries for empty streams")
			assert.Empty(t, entries, "got entries for empty streams")
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
				Block: -1,
			})

			id1 := addStreamEntry(t, c, stream1)

			var entries map[string][]StreamEntry
			assert.True(t, r.Next(&entries), "got no entries")
			assert.Len(t, entries, 1)
			assert.Len(t, entries[stream1], 1)
			assert.Equal(t, id1, entries[stream1][0].ID)

			id2 := addStreamEntry(t, c, stream2)

			assert.True(t, r.Next(&entries), "got no entries")
			assert.Len(t, entries, 2)
			assert.Len(t, entries[stream1], 0)
			assert.Len(t, entries[stream2], 1)
			assert.Equal(t, id2, entries[stream2][0].ID)

			assert.Zero(t, r.Next(&entries), "got entries for empty streams")
			assert.Len(t, entries, 2)
			assert.Len(t, entries[stream1], 0)
			assert.Len(t, entries[stream2], 0)
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
				Count: 2,
				Block: -1,
			})

			addNStreamEntries(t, c, stream1, 5)
			addNStreamEntries(t, c, stream2, 3)

			var entries map[string][]StreamEntry

			assert.True(t, r.Next(&entries))
			assert.Len(t, entries, 2)
			assert.Len(t, entries[stream1], 2)
			assert.Len(t, entries[stream2], 2)

			assert.True(t, r.Next(&entries))
			assert.Len(t, entries, 2)
			assert.Len(t, entries[stream1], 2)
			assert.Len(t, entries[stream2], 1)

			assert.True(t, r.Next(&entries))
			assert.Len(t, entries, 2)
			assert.Len(t, entries[stream1], 1)
			assert.Len(t, entries[stream2], 0)

			assert.False(t, r.Next(&entries))
			assert.Len(t, entries, 2)
			assert.Len(t, entries[stream1], 0)
			assert.Len(t, entries[stream2], 0)
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
			})

			var entries map[string][]StreamEntry
			assert.Zero(t, r.Next(&entries))
			err := r.Err()
			assert.Error(t, err)

			assert.Zero(t, r.Next(&entries))
			assert.Equal(t, err, r.Err())
		})

		t.Run("LatestEntries", func(t *T) {
			c := dial()
			defer c.Close()

			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: nil,
				},
				Block: 0,
			})

			var entries map[string][]StreamEntry

			idChan := make(chan StreamEntryID, 1)
			go func() {
				c := dial()
				defer c.Close()

				idChan <- addStreamEntry(t, c, stream)
			}()

			assert.True(t, r.Next(&entries))
			assert.Len(t, entries[stream], 1)
			id := <-idChan
			assert.Equal(t, id, entries[stream][0].ID)
			assert.NoError(t, r.Err())

			r = NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: &id,
				},
				Block: 0,
			})

			go func() {
				c := dial()
				defer c.Close()

				idChan <- addStreamEntry(t, c, stream)
			}()

			assert.True(t, r.Next(&entries))
			assert.Len(t, entries[stream], 1)
			id = <-idChan
			assert.Equal(t, id, entries[stream][0].ID)
			assert.NoError(t, r.Err())
		})

		t.Run("Block", func(t *T) {
			c := dial()
			defer c.Close()

			stream := randStr()

			r := NewStreamReader(c, StreamReaderOpts{
				Streams: map[string]*StreamEntryID{
					stream: {Time: 0, Seq: 0},
				},
				Block: 500 * time.Millisecond,
			})

			start := time.Now()

			var entries map[string][]StreamEntry
			assert.False(t, r.Next(&entries))
			assert.NoError(t, r.Err())

			end := time.Now()

			// FIXME(nussjustin): Flacky
			assert.WithinDuration(t, start.Add(500 * time.Millisecond), end, 100 * time.Millisecond)

			idChan := make(chan StreamEntryID, 1)
			time.AfterFunc(100 * time.Millisecond, func() {
				c := dial()
				defer c.Close()

				idChan <- addStreamEntry(t, c, stream)
			})

			assert.True(t, r.Next(&entries))
			assert.Equal(t, <-idChan, entries[stream][0].ID)
			assert.NoError(t, r.Err())
		})
	})
}

func BenchmarkStreamReader(b *B) {
	c := dial()
	defer c.Close()

	stream := randStr()

	firstID := addStreamEntry(b, c, stream).String()
	addNStreamEntries(b, c, stream, 31)

	b.Run("Group", func(b *B) {
		group := randStr()
		require.NoError(b, c.Do(Cmd(nil, "XGROUP", "CREATE", stream, group, firstID)))

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			require.NoError(b, c.Do(Cmd(nil, "XGROUP", "SETID", stream, group, firstID)))
			b.StartTimer()

			b.Skip("not implemented yet")
		}
	})

	b.Run("NoGroup", func(b *B) {
		entries := make(map[string][]StreamEntry)
		streams := map[string]*StreamEntryID{
			stream: {Time: 0, Seq: 0},
		}

		for i := 0; i < b.N; i++ {
			r := NewStreamReader(c, StreamReaderOpts{
				Streams: streams,
				Block: -1,
			})

			for r.Next(&entries) {
				benchErr = r.Err()
			}
			benchErr = r.Err()
		}
	})
}

func addStreamEntry(tb TB, c Client, stream string) StreamEntryID {
	var id StreamEntryID
	require.NoError(tb, c.Do(Cmd(&id, "XADD", stream, "*", randStr(), randStr())))
	return id
}

func addNStreamEntries(tb TB, c Client, stream string, n int) {
	for i := 0; i < n; i++ {
		addStreamEntry(tb, c, stream)
	}
}