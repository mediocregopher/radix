package bytesutil

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

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

var BytePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	},
}

func GetBytes() *[]byte {
	return BytePool.Get().(*[]byte)
}

func PutBytes(b *[]byte) {
	(*b) = (*b)[:0]
	BytePool.Put(b)
}

// parseInt is a specialized version of strconv.ParseInt that parses a base-10 encoded signed integer.
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

// parseUint is a specialized version of strconv.ParseUint that parses a base-10 encoded integer
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

func Expand(b []byte, to int) []byte {
	if cap(b) < to {
		nb := make([]byte, to)
		copy(nb, b)
		return nb
	}
	return b[:to]
}

// effectively an assert that the reader data starts with the given slice,
// discarding the slice at the same time
func BufferedPrefix(br *bufio.Reader, prefix []byte) error {
	b, err := br.Peek(len(prefix))
	if err != nil {
		return err
	} else if !bytes.Equal(b, prefix) {
		return fmt.Errorf("expected prefix %q, got %q", prefix, b)
	}
	_, err = br.Discard(len(prefix))
	return err
}

// reads bytes up to a delim and returns them, or an error
func BufferedBytesDelim(br *bufio.Reader) ([]byte, error) {
	b, err := br.ReadSlice('\n')
	if err != nil {
		return nil, err
	} else if len(b) < 2 {
		return nil, fmt.Errorf("malformed resp %q", b)
	}
	return b[:len(b)-2], err
}

// reads an integer out of the buffer, followed by a delim. It parses the
// integer, or returns an error
func BufferedIntDelim(br *bufio.Reader) (int64, error) {
	b, err := BufferedBytesDelim(br)
	if err != nil {
		return 0, err
	}
	return ParseInt(b)
}

// readNAppend appends exactly n bytes from r into b.
func ReadNAppend(r io.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 {
		return b, nil
	}
	m := len(b)
	b = Expand(b, len(b)+n)
	_, err := io.ReadFull(r, b[m:])
	return b, err
}

func ReadNDiscard(r io.Reader, n int) error {
	type discarder interface {
		Discard(int) (int, error)
	}

	if n == 0 {
		return nil
	} else if d, ok := r.(discarder); ok {
		_, err := d.Discard(n)
		return err
	}

	scratch := GetBytes()
	defer PutBytes(scratch)
	*scratch = (*scratch)[:cap(*scratch)]
	if len(*scratch) < n {
		// Large strings should get read in as bulk strings, in which case
		// *bufio.Reader will be read from directly, and so Discard will be
		// used. Any other kind of strings shouldn't be more than this.
		*scratch = make([]byte, 8192)
	}

	for {
		buf := *scratch
		if len(buf) > n {
			buf = buf[:n]
		}
		nr, err := r.Read(buf)
		n -= nr
		if n == 0 || err != nil {
			return err
		}
	}
}

func MultiWrite(w io.Writer, bb ...[]byte) error {
	for _, b := range bb {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func ReadInt(r io.Reader, n int) (int64, error) {
	scratch := GetBytes()
	defer PutBytes(scratch)

	var err error
	if *scratch, err = ReadNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	return ParseInt(*scratch)
}

func ReadUint(r io.Reader, n int) (uint64, error) {
	scratch := GetBytes()
	defer PutBytes(scratch)

	var err error
	if *scratch, err = ReadNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	return ParseUint(*scratch)
}

func ReadFloat(r io.Reader, precision, n int) (float64, error) {
	scratch := GetBytes()
	defer PutBytes(scratch)

	var err error
	if *scratch, err = ReadNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(*scratch), precision)
}

