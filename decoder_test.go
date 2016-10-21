package radix

import (
	"bytes"
	"errors"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type textCPUnmarshaller []byte

func (cu *textCPUnmarshaller) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCPUnmarshaller []byte

func (cu *binCPUnmarshaller) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

var decodeTests = []struct {
	in, out string
	outI    int64
	outF    float64
}{
	// Simple string
	//{in: "+\r\n", out: ""},
	//{in: "+ohey\r\n", out: "ohey"},
	//{in: "+10\r\n", out: "10", outI: 10, outF: 10},
	//{in: "+10.5\r\n", out: "10.5", outF: 10.5},

	// Int
	//{in: ":1024\r\n", out: "1024", outI: 1024, outF: 1024},

	// Bulk string
	//{in: "$0\r\n\r\n", out: ""},
	{in: "$4\r\nohey\r\n", out: "ohey"},
}

func TestDecode(t *T) {
	buf := new(bytes.Buffer)
	d := NewDecoder(buf)

	for _, dt := range decodeTests {
		doDecode := func(v interface{}) {
			buf.WriteString(dt.in)
			require.Nil(t, d.Decode(v))
		}

		{
			doDecode(nil)
			assert.Zero(t, len(buf.Bytes()))
		}

		{
			w := new(bytes.Buffer)
			doDecode(w)
			assert.Equal(t, dt.out, w.String())
		}

		{
			var s string
			doDecode(&s)
			assert.Equal(t, dt.out, s)
		}

		{
			// we make a byte slice and add one bogus character to it, so we can
			// ensure a new slice wasn't allocated
			b := make([]byte, 0, 1024)
			b = append(b, '_')
			bTail := b[1:]
			doDecode(&bTail)
			b = b[:1+len(bTail)] // expand b to encompass the tail
			assert.Equal(t, "_"+dt.out, string(b))
		}

		{
			// make a byte slice so small it'll _have_ to be reallocated
			// (usually)
			b := make([]byte, 0, 1)
			doDecode(&b)
			assert.Equal(t, dt.out, string(b))
		}

		{
			var cu textCPUnmarshaller
			doDecode(&cu)
			assert.Equal(t, dt.out, string(cu))
		}

		{
			var cu binCPUnmarshaller
			doDecode(&cu)
			assert.Equal(t, dt.out, string(cu))
		}

		if dt.outI > 0 {
			var i int64
			doDecode(&i)
			assert.Equal(t, dt.outI, i)

			var ui uint64
			doDecode(&ui)
			assert.Equal(t, uint64(dt.outI), ui)
		}

		if dt.outF > 0 {
			var f32 float32
			doDecode(&f32)
			assert.Equal(t, float32(dt.outF), f32)

			var f64 float64
			doDecode(&f64)
			assert.Equal(t, dt.outF, f64)
		}
	}
}

func TestDecodeAppErr(t *T) {
	// AppErr is handled separate from everything else, so it gets its own test
	buf := bytes.NewBufferString("-ohey\r\n")
	d := NewDecoder(buf)
	err := d.Decode(nil)
	assert.Equal(t, AppErr{errors.New("ohey")}, err)
}
