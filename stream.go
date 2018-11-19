package radix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/mediocregopher/radix/v3/internal/bytesutil"
	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/resp/resp2"
)

// StreamEntryID represents an ID used in a Redis stream with the format <time>-<seq>.
type StreamEntryID struct {
	// Time is the first part of the ID, which is based on the time of the server that Redis runs on.
	Time uint64

	// Seq is the sequence number of the ID for entries with the same Time value.
	Seq uint64
}

// Before returns true if s comes before o in a stream (is less than o).
func (s StreamEntryID) Before(o StreamEntryID) bool {
	if s.Time != o.Time {
		return s.Time < o.Time
	}

	return s.Seq < o.Seq
}

// Prev returns the previous stream entry ID or s if there is no prior id (s is 0-0).
func (s StreamEntryID) Prev() StreamEntryID {
	if s.Seq > 0 {
		s.Seq--
		return s
	}

	if s.Time > 0 {
		s.Time--
		s.Seq = math.MaxUint64
		return s
	}

	return s
}

// Next returns the next stream entry ID or s if there is no higher id (s is 18446744073709551615-18446744073709551615).
func (s StreamEntryID) Next() StreamEntryID {
	if s.Seq < math.MaxUint64 {
		s.Seq++
		return s
	}

	if s.Time < math.MaxUint64 {
		s.Time++
		s.Seq = 0
		return s
	}

	return s
}

var _ resp.Marshaler = (*StreamEntryID)(nil)
var _ resp.Unmarshaler = (*StreamEntryID)(nil)

const maxUint64Len = len("18446744073709551615")

// MarshalRESP implements the resp.Marshaler interface.
func (s *StreamEntryID) MarshalRESP(w io.Writer) error {
	var buf [maxUint64Len*2 + 1]byte
	b := strconv.AppendUint(buf[:0], s.Time, 10)
	b = append(b, '-')
	b = strconv.AppendUint(b, s.Seq, 10)

	return resp2.BulkStringBytes{B: b}.MarshalRESP(w)
}

var errInvalidStreamID = errors.New("invalid stream entry id")

// UnmarshalRESP implements the resp.Unmarshaler interface.
func (s *StreamEntryID) UnmarshalRESP(br *bufio.Reader) error {
	buf := bytesutil.GetBytes()
	defer bytesutil.PutBytes(buf)

	bsb := resp2.BulkStringBytes{B: (*buf)[:0]}
	if err := bsb.UnmarshalRESP(br); err != nil {
		return err
	}

	split := bytes.IndexByte(bsb.B, '-')
	if split == -1 {
		return errInvalidStreamID
	}

	time, err := bytesutil.ParseUint(bsb.B[:split])
	if err != nil {
		return errInvalidStreamID
	}

	seq, err := bytesutil.ParseUint(bsb.B[split+1:])
	if err != nil {
		return errInvalidStreamID
	}

	s.Time, s.Seq = time, seq
	return nil
}

var _ fmt.Stringer = (*StreamEntryID)(nil)

// String returns the ID in the format <time>-<seq> (the same format used by Redis).
//
// String implements the fmt.Stringer interface.
func (s StreamEntryID) String() string {
	var buf [maxUint64Len*2 + 1]byte
	b := strconv.AppendUint(buf[:0], s.Time, 10)
	b = append(b, '-')
	b = strconv.AppendUint(b, s.Seq, 10)
	return string(b)
}

// StreamEntry is an entry in a Redis stream as returned by XRANGE, XREAD and XREADGROUP.
type StreamEntry struct {
	// ID is the ID of the entry in a stream.
	ID StreamEntryID

	// Fields contains the fields and values for the stream entry.
	Fields map[string]string
}

var _ resp.Unmarshaler = (*StreamEntry)(nil)

var errInvalidStreamEntry = errors.New("invalid stream entry")

// UnmarshalRESP implements the resp.Unmarshaler interface.
func (s *StreamEntry) UnmarshalRESP(br *bufio.Reader) error {
	var ah resp2.ArrayHeader
	if err := ah.UnmarshalRESP(br); err != nil {
		return err
	}
	if ah.N != 2 {
		return errInvalidStreamEntry
	}

	if err := s.ID.UnmarshalRESP(br); err != nil {
		return err
	}

	return s.unmarshalFields(br)
}

func (s *StreamEntry) unmarshalFields(br *bufio.Reader) error {
	// resp2.Any{I: &s.Fields}.UnmarshalRESP(br)
	var ah resp2.ArrayHeader
	if err := ah.UnmarshalRESP(br); err != nil {
		return err
	}
	if ah.N % 2 != 0 {
		return errInvalidStreamEntry
	}

	if s.Fields == nil {
		s.Fields = make(map[string]string, ah.N / 2)
	} else {
		for k := range s.Fields {
			delete(s.Fields, k)
		}
	}

	var bs resp2.BulkString
	for i := 0; i < ah.N; i+= 2 {
		if err := bs.UnmarshalRESP(br); err != nil {
			return err
		}
		key := bs.S
		if err := bs.UnmarshalRESP(br); err != nil {
			return err
		}
		s.Fields[key] = bs.S
	}
	return nil
}

// StreamReaderOpts contains various options given for NewStreamReader that influence the behaviour.
//
// The only required field is Streams.
type StreamReaderOpts struct {
	// Streams must contain one or more stream names that will be read.
	//
	// The value for each stream can either be nil or an existing ID.
	// If a value is non-nil, only newer stream entries will be returned.
	Streams map[string]*StreamEntryID

	// Group is an optional consumer group name.
	//
	// If Group is not empty reads will use XREADGROUP with the Group as consumer group instead of XREAD.
	Group string

	// Consumer is an optional consumer name for use with Group.
	//
	// TODO(nussjustin): Automatically detect if empty and Group is set?
	Consumer string

	// Noack optionally enables passing the NOACK flag to XREADGROUP.
	Noack bool

	// Block specifies the duration in milliseconds that reads will wait for new data before returning.
	//
	// If Block is negative, reads will not block (no BLOCK option is set) when there is no new data to read.
	//
	// TODO(nussjustin): Make no blocking mode default? How to specify 0 milliseconds (infinite blocking)?
	Block time.Duration

	// Count can be used to limit the number of entries retrieved by each call to Next.
	//
	// If Count is 0, all available entries will be retrieved.
	Count int
}

// StreamReader allows reading from on or more streams, always returning newer entries
type StreamReader interface {
	// Err returns any error that happened while calling Next or nil if no error happened.
	//
	// Once Err returns a non-nil error, all successive calls will return the same error.
	Err() error

	// Next reads the next batch of stream entries from the Client into entries.
	//
	// If *entries is nil, a new map will be allocated. If *entries is not empty, Next will
	// override the values for all streams, even if no new entries were returned.
	//
	// Next returns the number of streams with new entries or 0 if there was an error.
	//
	// Once an error occured (Err() != nil), all future calls to Next will return 0.
	Next(entries *map[string][]StreamEntry) int
}

// NewStreamReader returns a new StreamReader for the given client.
//
// Any changes on opts after calling NewStreamReader will have no effect.
func NewStreamReader(c Client, opts StreamReaderOpts) StreamReader {
	sr := &streamReader{c: c, opts: opts}
	sr.init()
	return sr
}

// streamReader implements the StreamReader interface.
type streamReader struct {
	c    Client
	opts StreamReaderOpts // copy of the options given to NewStreamReader with Streams == nil

	streams []string
	ids map[string]string

	cmd       string   // command. either XREAD or XREADGROUP
	fixedArgs []string // fixed arguments that always come directly after the command
	args      []string // arguments passed to Cmd. reused between calls to Next to avoid allocations.

	unmarshaler streamReaderUnmarshaler
	err error
}

func (sr *streamReader) init() {
	if sr.opts.Group != "" {
		sr.cmd = "XREADGROUP"
		sr.fixedArgs = []string{"GROUP", sr.opts.Group, sr.opts.Consumer}
	} else {
		sr.cmd = "XREAD"
		sr.fixedArgs = nil
	}

	if sr.opts.Count > 0 {
		sr.fixedArgs = append(sr.fixedArgs, "COUNT", strconv.Itoa(sr.opts.Count))
	}

	if sr.opts.Block >= 0 {
		sec := int(sr.opts.Block / time.Millisecond)
		sr.fixedArgs = append(sr.fixedArgs, "BLOCK", strconv.Itoa(sec))
	}

	if sr.opts.Group != "" && sr.opts.Noack {
		sr.fixedArgs = append(sr.fixedArgs, "NOACK")
	}

	sr.streams = make([]string, 0, len(sr.opts.Streams))
	sr.ids = make(map[string]string, len(sr.opts.Streams))
	for stream, id := range sr.opts.Streams {
		sr.streams = append(sr.streams, stream)

		if id != nil {
			sr.ids[stream] = id.String()
		} else if sr.cmd == "XREAD" {
			sr.ids[stream] = "$"
		} else if sr.cmd == "XREADGROUP" {
			sr.ids[stream] = ">"
		}
	}

	// set to nil so we don't accidentally use it later, since the user could have changed
	// the map while after using the reader.
	sr.opts.Streams = nil

	sr.fixedArgs = append(sr.fixedArgs, "STREAMS")
	sr.fixedArgs = append(sr.fixedArgs, sr.streams...)

	// preallocate space for all arguments passed to Cmd
	sr.args = make([]string, 0, len(sr.fixedArgs)+len(sr.streams))
}

func (sr *streamReader) do() error {
	sr.args = append(sr.args[:0], sr.fixedArgs...)

	for _, s := range sr.streams {
		sr.args = append(sr.args, sr.ids[s])
	}

	return sr.c.Do(Cmd(&sr.unmarshaler, sr.cmd, sr.args...))
}

// Err implements the StreamReader interface.
func (sr *streamReader) Err() error {
	return sr.err
}

// Next implements the StreamReader interface.
func (sr *streamReader) Next(entries *map[string][]StreamEntry) int {
	if sr.err != nil {
		return 0
	}

	if *entries == nil {
		*entries = make(map[string][]StreamEntry, len(sr.streams))
	}
	for k, v := range *entries {
		(*entries)[k] = v[:0]
	}

	sr.unmarshaler.entries = *entries
	if sr.err = sr.do(); sr.err != nil {
		return 0
	}
	for k, v := range *entries {
		if len(v) > 0 {
			sr.ids[k] = v[len(v)-1].ID.String()
		}
	}
	return sr.unmarshaler.n
}

// streamReaderUnmarshaler implements unmarshaling of XREAD and XREADGROUP responses.
//
// The logic is not part of streamReader so that users can not use streamReader directly
// as a resp.Unmarshaler.
type streamReaderUnmarshaler struct {
	entries map[string][]StreamEntry
	n int // number of entries read
}

var _ resp.Unmarshaler = (*streamReaderUnmarshaler)(nil)

// UnmarshalRESP implements the resp.Unmarshaler interface.
func (s *streamReaderUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	s.n = 0

	var ah resp2.ArrayHeader
	if err := ah.UnmarshalRESP(br); err != nil {
		return err
	}
	if ah.N < 1 {
		return nil
	}

	for i := 0; i < ah.N; i++ {
		if err := s.unmarshalNamedStream(br); err != nil {
			return err
		}
	}

	s.n = ah.N
	return nil
}

func (s *streamReaderUnmarshaler) unmarshalNamedStream(br *bufio.Reader) error {
	var ah resp2.ArrayHeader
	if err := ah.UnmarshalRESP(br); err != nil {
		return err
	}
	if ah.N != 2 {
		return errors.New("invalid xread[group] response")
	}

	var nameBytes resp2.BulkStringBytes
	if err := nameBytes.UnmarshalRESP(br); err != nil {
		return err
	}

	return s.unmarshalEntries(br, nameBytes.B)
}

func (s *streamReaderUnmarshaler) unmarshalEntries(br *bufio.Reader, stream []byte) error {
	var ah resp2.ArrayHeader
	if err := ah.UnmarshalRESP(br); err != nil {
		return err
	}
	if ah.N < 0 {
		return errors.New("invalid xread[group] response")
	}

	// m[string(k)] does not allocate, so this avoids allocating a string when there is
	// already a value with the correct key in the map.
	entries := s.entries[string(stream)]
	if cap(entries) < ah.N {
		entries = make([]StreamEntry, ah.N)
	} else {
		entries = entries[:ah.N]
	}
	s.entries[string(stream)] = entries

	for i := range entries {
		if err := entries[i].UnmarshalRESP(br); err != nil {
			return err
		}
	}

	return nil
}