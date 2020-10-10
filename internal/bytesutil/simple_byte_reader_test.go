package bytesutil

import (
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleByteReader(t *testing.T) {
	type test struct {
		in           []byte
		l            int
		exp, expLeft []byte
		expErr       bool
	}

	tests := []test{
		{
			in: []byte{},
			l:  1, exp: []byte{}, expLeft: []byte{},
			expErr: true,
		},
		{
			in: []byte("f"),
			l:  0, exp: []byte(""), expLeft: []byte("f"),
		},
		{
			in: []byte("f"),
			l:  1, exp: []byte("f"), expLeft: []byte("\n"),
			expErr: true,
		},
		{
			in: []byte("\n"),
			l:  1, exp: []byte(""), expLeft: []byte("\n"),
			expErr: true,
		},
		{
			in: []byte("foo"),
			l:  3, exp: []byte("foo"), expLeft: []byte("\noo"),
			expErr: true,
		},
		{
			in: []byte("foo"),
			l:  2, exp: []byte("fo"), expLeft: []byte("oo\n"),
		},
		{
			in: []byte("foo"),
			l:  1, exp: []byte("f"), expLeft: []byte("o\no"),
		},
		{
			in: []byte("o\no"),
			l:  2, exp: []byte("o"), expLeft: []byte("\n\no"),
		},
		{
			in: []byte("o\no"),
			l:  1, exp: []byte("o"), expLeft: []byte("\n\no"),
		},
		{
			in: []byte("o\no"),
			l:  0, exp: []byte(""), expLeft: []byte("o\no"),
		},
		{
			in: []byte("\n\no"),
			l:  1, exp: []byte(""), expLeft: []byte("\n\no"),
			expErr: true,
		},
		{
			in: []byte("\n\no"),
			l:  0, exp: []byte(""), expLeft: []byte("\n\no"),
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			br := SimpleByteReader(test.in)
			into := make([]byte, test.l)
			n, err := br.Read(into)
			assert.Equal(t, test.exp, into[:n])
			assert.Equal(t, test.expLeft, []byte(br))
			if test.expErr {
				assert.Equal(t, io.EOF, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
