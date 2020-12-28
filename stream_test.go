package radix

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	. "testing"
	"time"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamEntryID(t *T) {
	opts := resp.NewOpts()
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
			require.Nil(t, test.S.MarshalRESP(&buf, opts))
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
			err := s.UnmarshalRESP(br, opts)

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
	opts := resp.NewOpts()
	b.Run("String", func(b *B) {
		s := &StreamEntryID{Time: 1234, Seq: 5678}

		for i := 0; i < b.N; i++ {
			benchString = s.String()
		}
	})

	b.Run("MarshalRESP", func(b *B) {
		s := &StreamEntryID{Time: 1234, Seq: 5678}

		for i := 0; i < b.N; i++ {
			benchErr = s.MarshalRESP(ioutil.Discard, opts)
		}
	})

	b.Run("UnmarshalRESP", func(b *B) {
		s := &StreamEntryID{Time: 1234, Seq: 5678}

		buf := strings.NewReader("")
		br := bufio.NewReader(buf)

		for i := 0; i < b.N; i++ {
			buf.Reset("$9\r\n1234-5678\r\n")
			br.Reset(buf)

			benchErr = s.UnmarshalRESP(br, opts)
		}
	})
}

func TestStreamEntry(t *T) {
	ctx := testCtx(t)
	c := dial()
	defer c.Close()

	stream := randStr()

	var id1, id2 string
	require.NoError(t, c.Do(ctx, Cmd(&id1, "XADD", stream, "*", "hello", "world", "foo", "bar")))
	require.NoError(t, c.Do(ctx, Cmd(&id2, "XADD", stream, "*", "hello", "bar")))

	var entries []StreamEntry
	assert.NoError(t, c.Do(ctx, Cmd(&entries, "XRANGE", stream, "-", "+")))
	assert.Len(t, entries, 2, "got wrong number of stream entries")

	assert.Equal(t, id1, entries[0].ID.String(), "parsed ID differs from ID returned by XADD")
	assert.Equal(t, [][2]string{{"hello", "world"}, {"foo", "bar"}}, entries[0].Fields)

	assert.Equal(t, id2, entries[1].ID.String(), "parsed ID differs from ID returned by XADD")
	assert.Equal(t, [][2]string{{"hello", "bar"}}, entries[1].Fields)

	assert.True(t, entries[0].ID.Before(entries[1].ID))

	t.Run("UnmarshalRESP", func(t *T) {
		for _, test := range []struct {
			In  string
			E   StreamEntry
			Err string
		}{
			{
				In: "*2\r\n$3\r\n1-1\r\n*-1\r\n",
				E: StreamEntry{
					ID:     StreamEntryID{Time: 1, Seq: 1},
					Fields: nil,
				},
			},
		} {
			br := bufio.NewReader(strings.NewReader(test.In))
			opts := resp.NewOpts()

			var s StreamEntry
			err := s.UnmarshalRESP(br, opts)

			if test.Err == "" {
				assert.NoErrorf(t, err, "failed to unmarshal %q", test.In)
			} else {
				assert.EqualError(t, err, test.Err)
			}

			assert.Equal(t, test.E, s)
		}
	})

}

func BenchmarkStreamEntry(b *B) {
	ctx := testCtx(b)
	c := dial()
	defer c.Close()

	stream := randStr()
	require.NoError(b, c.Do(ctx, Cmd(nil, "XADD", stream, "*", "hello", "world", "foo", "bar")))

	b.ResetTimer()

	// preallocate so we don't measure the slice allocation cost
	entries := make([]StreamEntry, 0, 1)

	for i := 0; i < b.N; i++ {
		entries = entries[:0]
		benchErr = c.Do(ctx, Cmd(&entries, "XRANGE", stream, "-", "+"))
	}
}

func TestStreamReader(t *T) {
	assertRespErr := func(tb TB, expPrefix string, err error) {
		var respErr resp3.SimpleError
		assert.True(t, errors.As(err, &respErr), "err:%v", err)
		assert.True(t, strings.HasPrefix(respErr.S, expPrefix))
	}

	t.Run("Group", func(t *T) {
		t.Run("Empty", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream1, stream2 := randStr(), randStr()

			r := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream1: {},
				stream2: {},
			})

			addStreamGroup(t, c, stream1, group, "0-0")
			addStreamGroup(t, c, stream2, group, "0-0")
			assertNoStreamReaderEntries(t, r)
		})

		t.Run("NoGroup", func(t *T) {
			ctx := testCtx(t)
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := (StreamReaderConfig{
				Group:   randStr(),
				NoBlock: true,
			}).New(c, map[string]StreamConfig{
				stream1: {},
				stream2: {},
			})

			_, _, err := r.Next(ctx)
			assertRespErr(t, "NOGROUP No such key", err)
		})

		t.Run("EOS", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream1, stream2 := randStr(), randStr()

			r := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream1: {Latest: true},
				stream2: {Latest: true},
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

			r := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				Count:    2,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream1: {Latest: true},
				stream2: {Latest: true},
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
			ctx := testCtx(t)
			c := dial()
			defer c.Close()

			stream1 := randStr()

			require.NoError(t, c.Do(ctx, Cmd(nil, "SET", stream1, "1")))

			r := (StreamReaderConfig{
				Group:    randStr(),
				Consumer: randStr(),
			}).New(c, map[string]StreamConfig{
				stream1: {Latest: true},
			})

			_, _, err := r.Next(ctx)
			assertRespErr(t, "WRONGTYPE ", err)

			_, _, err = r.Next(ctx)
			assertRespErr(t, "WRONGTYPE ", err)
		})

		t.Run("Blocking", func(t *T) {
			ctx := testCtx(t)
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				Count:    10,
			}).New(c, map[string]StreamConfig{
				stream: {Latest: true},
			})

			addStreamGroup(t, c, stream, group, "0-0")

			for i := 1; i <= 2; i++ {
				idChan := make(chan StreamEntryID, 1)
				time.AfterFunc(100*time.Millisecond, func() {
					c := dial()
					defer c.Close()
					idChan <- addStreamEntry(t, c, stream)
				})

				ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
				_, entry, err := r.Next(ctx)
				cancel()

				id := <-idChan
				assert.NoError(t, err)
				assert.Equal(t, id, entry.ID)

				assertConsumer(t, c, stream, group, consumer, i)
			}
		})

		t.Run("NoAck", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
				NoAck:    true,
			}).New(c, map[string]StreamConfig{
				stream: {Latest: true},
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

			r1 := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream: {Latest: true},
			})

			addStreamGroup(t, c, stream, group, "0-0")

			assertNoStreamReaderEntries(t, r1)
			id1 := addStreamEntry(t, c, stream)
			assertStreamReaderEntries(t, r1, map[string][]StreamEntryID{stream: {id1}})
			id2 := addStreamEntry(t, c, stream)

			r2 := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream: {},
			})

			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id1}})
			assertConsumer(t, c, stream, group, consumer, 1)

			assertStreamReaderEntries(t, r1, map[string][]StreamEntryID{stream: {id2}})
			assertNoStreamReaderEntries(t, r1)

			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id2}})
			assertNoStreamReaderEntries(t, r2)
			assertConsumer(t, c, stream, group, consumer, 2)
		})

		t.Run("CountWithRefill", func(t *T) {
			ctx := testCtx(t)
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream1, stream2 := randStr(), randStr()

			s1id1 := addStreamEntry(t, c, stream1)
			s1id2 := addStreamEntry(t, c, stream1)
			s1id3 := addStreamEntry(t, c, stream1)
			s2id1 := addStreamEntry(t, c, stream2)

			addStreamGroup(t, c, stream1, group, "0")
			addStreamGroup(t, c, stream2, group, "0")

			// read all entries once to make them unacknowledged
			{
				r := (StreamReaderConfig{
					NoBlock:  true,
					Group:    group,
					Consumer: consumer,
				}).New(c, map[string]StreamConfig{
					stream1: {Latest: true},
					stream2: {Latest: true},
				})
				for {
					if _, _, err := r.Next(ctx); errors.Is(err, ErrNoStreamEntries) {
						break
					} else if err != nil {
						t.Fatal(err)
					}
				}
			}

			r := (StreamReaderConfig{
				NoBlock:  true,
				Group:    group,
				Consumer: consumer,
				Count:    1,
			}).New(c, map[string]StreamConfig{
				stream1: {},
				stream2: {},
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: {s1id1},
				stream2: {s2id1},
			})

			assertStreamReaderEntries(t, r, map[string][]StreamEntryID{
				stream1: {s1id2, s1id3},
			})

			assertNoStreamReaderEntries(t, r)
		})

		t.Run("PendingThenLatest", func(t *T) {
			c := dial()
			defer c.Close()

			consumer, group := randStr(), randStr()
			stream := randStr()

			r1 := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream: {Latest: true},
			})

			addStreamGroup(t, c, stream, group, "0-0")

			assertNoStreamReaderEntries(t, r1)
			id1 := addStreamEntry(t, c, stream)
			assertStreamReaderEntries(t, r1, map[string][]StreamEntryID{stream: {id1}})
			id2 := addStreamEntry(t, c, stream)

			r2 := (StreamReaderConfig{
				Group:    group,
				Consumer: consumer,
				NoBlock:  true,
			}).New(c, map[string]StreamConfig{
				stream: {PendingThenLatest: true},
			})

			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id1}})
			assertConsumer(t, c, stream, group, consumer, 1)

			assertStreamReaderEntries(t, r1, map[string][]StreamEntryID{stream: {id2}})
			assertNoStreamReaderEntries(t, r1)

			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id2}})
			assertNoStreamReaderEntries(t, r2)
			assertConsumer(t, c, stream, group, consumer, 2)

			id3 := addStreamEntry(t, c, stream)
			assertStreamReaderEntries(t, r2, map[string][]StreamEntryID{stream: {id3}})
			assertNoStreamReaderEntries(t, r2)
			assertConsumer(t, c, stream, group, consumer, 3)
		})
	})

	t.Run("NoGroup", func(t *T) {
		t.Run("Empty", func(t *T) {
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := (StreamReaderConfig{
				NoBlock: true,
			}).New(c, map[string]StreamConfig{
				stream1: {},
				stream2: {},
			})

			assertNoStreamReaderEntries(t, r)
		})

		t.Run("EOS", func(t *T) {
			c := dial()
			defer c.Close()

			stream1, stream2 := randStr(), randStr()

			r := (StreamReaderConfig{
				NoBlock: true,
			}).New(c, map[string]StreamConfig{
				stream1: {},
				stream2: {},
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

			r := (StreamReaderConfig{
				Count:   2,
				NoBlock: true,
			}).New(c, map[string]StreamConfig{
				stream1: {},
				stream2: {},
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
			ctx := testCtx(t)
			c := dial()
			defer c.Close()

			stream := randStr()

			require.NoError(t, c.Do(ctx, Cmd(nil, "SET", stream, "1")))

			r := (StreamReaderConfig{}).New(c, map[string]StreamConfig{
				stream: {Latest: true},
			})

			_, _, err := r.Next(ctx)
			assertRespErr(t, "WRONGTYPE ", err)

			_, _, err = r.Next(ctx)
			assertRespErr(t, "WRONGTYPE ", err)
		})

		t.Run("Blocking", func(t *T) {
			ctx := testCtx(t)
			c := dial()
			defer c.Close()

			stream := randStr()

			r := (StreamReaderConfig{
				Count: 10,
			}).New(c, map[string]StreamConfig{
				stream: {Latest: true},
			})

			for i := 0; i < 2; i++ {
				idChan := make(chan StreamEntryID, 1)
				time.AfterFunc(100*time.Millisecond, func() {
					c := dial()
					defer c.Close()
					idChan <- addStreamEntry(t, c, stream)
				})

				ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
				_, entry, err := r.Next(ctx)
				cancel()

				id := <-idChan
				assert.NoError(t, err)
				assert.Equal(t, id, entry.ID)
			}
		})
	})
}

func BenchmarkStreamReader(b *B) {
	ctx := testCtx(b)
	c := dial()
	defer c.Close()

	stream := randStr()
	streams := map[string]StreamConfig{
		stream: {},
	}

	addNStreamEntries(b, c, stream, 32)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := (StreamReaderConfig{NoBlock: true}).New(c, streams)
		for {
			if _, _, err := r.Next(ctx); err != nil && !errors.Is(err, ErrNoStreamEntries) {
				b.Fatal(err)
			}
		}
	}
}

func addStreamEntry(tb TB, c Client, stream string) StreamEntryID {
	tb.Helper()
	var id StreamEntryID
	require.NoError(tb, c.Do(testCtx(tb), Cmd(&id, "XADD", stream, "*", randStr(), randStr())))
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
	_, _, err := r.Next(testCtx(tb))
	assert.True(tb, errors.Is(err, ErrNoStreamEntries), "expected ErrNoStreamEntries, got %v", err)
}

func assertStreamReaderEntries(tb TB, r StreamReader, expected map[string][]StreamEntryID) {
	tb.Helper()

	for len(expected) > 0 {
		stream, entry, err := r.Next(testCtx(tb))
		if errors.Is(err, ErrNoStreamEntries) {
			break
		} else if err != nil {
			tb.Fatal(err)
		}

		expectedForStream := expected[stream]
		if len(expectedForStream) == 0 {
			assert.Failf(tb, "unexpected stream entries", "got entry for stream %s", stream)
			break
		}

		var found bool
		for i, want := range expectedForStream {
			if want != entry.ID {
				continue
			}
			expectedForStream = append(expectedForStream[:i], expectedForStream[i+1:]...)
			found = true
			break
		}
		if !found {
			assert.Failf(tb, "found unexpected entry", "%s in stream %s", entry.ID, stream)
			break
		}

		if len(expectedForStream) > 0 {
			expected[stream] = expectedForStream
		} else {
			delete(expected, stream)
		}
	}

	if len(expected) > 0 {
		assert.Failf(tb, "unexpected end of stream", "expected one of: %v", expected)
	}
}

func addStreamGroup(tb TB, c Client, stream, group, id string) {
	tb.Helper()
	require.NoError(tb, c.Do(testCtx(tb), Cmd(nil, "XGROUP", "CREATE", stream, group, id, "MKSTREAM")))
}

func assertConsumer(tb TB, c Client, stream, group, consumer string, pending int) {
	tb.Helper()
	var cs []map[string]string
	require.NoError(tb, c.Do(testCtx(tb), Cmd(&cs, "XINFO", "CONSUMERS", stream, group)))

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
