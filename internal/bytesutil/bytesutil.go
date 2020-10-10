// Package bytesutil provides utility functions for working with bytes and byte streams that are useful when
// working with the RESP protocol.
package bytesutil

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/mediocregopher/radix/v4/resp"
)

// AnyIntToInt64 converts a value of any of Go's integer types (signed and unsigned) into a signed int64.
//
// If m is not of one of Go's built in integer types the call will panic.
func AnyIntToInt64(m interface{}) int64 {
	switch mt := m.(type) {
	case int:
		return int64(mt)
	case int8:
		return int64(mt)
	case int16:
		return int64(mt)
	case int32:
		return int64(mt)
	case int64:
		return mt
	case uint:
		return int64(mt)
	case uint8:
		return int64(mt)
	case uint16:
		return int64(mt)
	case uint32:
		return int64(mt)
	case uint64:
		return int64(mt)
	}
	panic(fmt.Sprintf("anyIntToInt64 got bad arg: %#v", m))
}

var bytePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	},
}

// GetBytes returns a non-nil pointer to a byte slice from a pool of byte slices.
//
// The returned byte slice should be put back into the pool using PutBytes after usage.
func GetBytes() *[]byte {
	return bytePool.Get().(*[]byte)
}

// PutBytes puts the given byte slice pointer into a pool that can be accessed via GetBytes.
//
// After calling PutBytes the given pointer and byte slice must not be accessed anymore.
func PutBytes(b *[]byte) {
	*b = (*b)[:0]
	bytePool.Put(b)
}

// ParseInt is a specialized version of strconv.ParseInt that parses a base-10
// encoded signed integer from a []byte.
//
// This can be used to avoid allocating a string, since strconv.ParseInt only
// takes a string.
func ParseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty slice given to parseInt")
	}

	var neg bool
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		b = b[1:]
	}

	n, err := ParseUint(b)
	if err != nil {
		return 0, err
	}

	if neg {
		return -int64(n), nil
	}

	return int64(n), nil
}

// ParseUint is a specialized version of strconv.ParseUint that parses a base-10
// encoded integer from a []byte.
//
// This can be used to avoid allocating a string, since strconv.ParseUint only
// takes a string.
func ParseUint(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty slice given to parseUint")
	}

	var n uint64

	for i, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid character %c at position %d in parseUint", c, i)
		}

		n *= 10
		n += uint64(c - '0')
	}

	return n, nil
}

func expand(b []byte, n int, keepBytes bool) []byte {
	if n == 0 && b == nil {
		b = []byte{} // so as to not return nil
	} else if cap(b) < n {
		nb := make([]byte, n)
		if keepBytes {
			copy(nb, b)
		}
		return nb
	}
	return b[:n]
}

// Expand expands the given byte slice to exactly n bytes. It will not return
// nil.
//
// If cap(b) < n then a new slice will be allocated.
func Expand(b []byte, n int) []byte {
	return expand(b, n, false)
}

// ReadBytesDelim reads a line from br and checks that the line ends with
// \r\n, returning the line without \r\n.
func ReadBytesDelim(br *bufio.Reader) ([]byte, error) {
	b, err := br.ReadSlice('\n')
	if err != nil {
		return nil, err
	} else if len(b) < 2 || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("malformed resp %q", b)
	}
	return b[:len(b)-2], err
}

// ReadIntDelim reads the current line from br as an integer, checks that the
// line ends with \r\n, and returns the integer.
func ReadIntDelim(br *bufio.Reader) (int64, error) {
	b, err := ReadBytesDelim(br)
	if err != nil {
		return 0, err
	}
	return ParseInt(b)
}

// ReadNAppend appends exactly n bytes from r into b.
func ReadNAppend(r io.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 {
		return b, nil
	}
	m := len(b)
	b = expand(b, len(b)+n, true)
	_, err := io.ReadFull(r, b[m:])
	return b, err
}

// ReadNDiscard discards exactly n bytes from r.
func ReadNDiscard(r io.Reader, n int) error {
	type discarder interface {
		Discard(int) (int, error)
	}

	if n == 0 {
		return nil
	}

	switch v := r.(type) {
	case discarder:
		_, err := v.Discard(n)
		return err
	case io.Seeker:
		_, err := v.Seek(int64(n), io.SeekCurrent)
		return err
	}

	scratch := GetBytes()
	defer PutBytes(scratch)
	*scratch = (*scratch)[:cap(*scratch)]
	if len(*scratch) < n {
		*scratch = make([]byte, 8192)
	}

	for {
		buf := *scratch
		if len(buf) > n {
			buf = buf[:n]
		}
		nr, err := r.Read(buf)
		n -= nr
		if n == 0 {
			return nil
		} else if err == io.EOF {
			return io.ErrUnexpectedEOF
		} else if err != nil {
			return err
		}
	}
}

// ReadInt reads the next n bytes from r as a signed 64 bit integer.
func ReadInt(r io.Reader, n int) (int64, error) {
	scratch := GetBytes()
	defer PutBytes(scratch)

	var err error
	if *scratch, err = ReadNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	i, err := ParseInt(*scratch)
	if err != nil {
		return 0, resp.ErrConnUsable{Err: err}
	}
	return i, nil
}

// ReadUint reads the next n bytes from r as an unsigned 64 bit integer.
func ReadUint(r io.Reader, n int) (uint64, error) {
	scratch := GetBytes()
	defer PutBytes(scratch)

	var err error
	if *scratch, err = ReadNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	ui, err := ParseUint(*scratch)
	if err != nil {
		return 0, resp.ErrConnUsable{Err: err}
	}
	return ui, nil
}

// ReadFloat reads the next n bytes from r as a 64 bit floating point number with the given precision.
func ReadFloat(r io.Reader, precision, n int) (float64, error) {
	scratch := GetBytes()
	defer PutBytes(scratch)

	var err error
	if *scratch, err = ReadNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(string(*scratch), precision)
	if err != nil {
		return 0, resp.ErrConnUsable{Err: err}
	}
	return f, nil
}
