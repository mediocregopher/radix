package radix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"io"
	"math"
	"strconv"
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
	var buf [maxUint64Len*2 + 1]byte

	bsb := resp2.BulkStringBytes{B: buf[:0]}
	if err := bsb.UnmarshalRESP(br); err != nil {
		return err
	}

	split := bytes.IndexByte(bsb.B, '-')
	if split == -1 {
		return errInvalidStreamID
	}

	// TODO(nussjustin): use readUint from the resp2 package
	time, err := strconv.ParseUint(string(bsb.B[:split]), 10, 64)
	if err != nil {
		return errInvalidStreamID
	}

	// TODO(nussjustin): use readUint from the resp2 package
	seq, err := strconv.ParseUint(string(bsb.B[split+1:]), 10, 64)
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

	return resp2.Any{I: &s.Fields}.UnmarshalRESP(br)
}