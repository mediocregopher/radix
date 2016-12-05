package resp

import (
	"bufio"
	"io"
)

// LenReader adds an additional method to io.Reader, returning how many bytes
// are left till be read until an io.EOF is reached.
type LenReader interface {
	io.Reader
	Len() int64
}

type lenReader struct {
	r io.Reader
	l int64
}

// NewLenReader wraps an existing io.Reader whose length is known so that it
// implements LenReader
func NewLenReader(r io.Reader, l int64) LenReader {
	return &lenReader{r: r, l: l}
}

func (lr *lenReader) Read(b []byte) (int, error) {
	n, err := lr.r.Read(b)
	lr.l -= int64(n)
	return n, err
}

func (lr *lenReader) Len() int64 {
	return lr.l
}

// A special limited reader which will read an extra two bytes after the limit
// has been reached
type limitedReaderPlus struct {
	eof bool
	lr  *io.LimitedReader
}

func newLimitedReaderPlus(br *bufio.Reader, limit int64) *limitedReaderPlus {
	return &limitedReaderPlus{
		lr: &io.LimitedReader{
			R: br,
			N: limit,
		},
	}
}

func (lrp *limitedReaderPlus) Read(b []byte) (int, error) {
	if lrp.eof {
		return 0, io.EOF
	}

	i, err := lrp.lr.Read(b)
	if err == io.EOF {
		lrp.eof = true
		_, err = lrp.lr.R.(*bufio.Reader).Discard(2)
		return i, err
	}
	return i, err
}
