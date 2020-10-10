package bytesutil

import (
	"io"
)

// SimpleByteReader wraps a []byte such that it implements the io.Reader
// interface. SimpleByteReader uses a '\n' character to track where it's read up
// to in the slice. For this reason it cannot be used with slices which contain
// a '\n' character.
type SimpleByteReader []byte

var _ io.Reader = SimpleByteReader{}

// Read implements the method for the io.Reader interface.
func (r SimpleByteReader) Read(b []byte) (int, error) {
	if len(r) == 0 {
		return 0, io.EOF
	} else if len(b) == 0 {
		return 0, nil
	}

	n := copy(b, r)
	for i := range b[:n] {
		if b[i] == '\n' {
			n = i
			break
		}
	}

	if n == 0 {
		return 0, io.EOF
	}

	// did not reach end yet, shift slice down and set \n marker
	copy(r, r[n:])
	if n == len(r) {
		r[0] = '\n'
		return n, io.EOF
	}
	r[n] = '\n'
	return n, nil
}
