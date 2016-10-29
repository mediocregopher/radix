package radix

import (
	"bytes"
	"errors"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type textCPMarshaler []byte

func (cm textCPMarshaler) MarshalText() ([]byte, error) {
	var b []byte
	b = append(b, '_')
	b = append(b, cm...)
	b = append(b, '_')
	return b, nil
}

type binCPMarshaler []byte

func (cm binCPMarshaler) MarshalBinary() ([]byte, error) {
	var b []byte
	b = append(b, '_')
	b = append(b, cm...)
	b = append(b, '_')
	return b, nil
}

var encodeTests = []struct {
	in  interface{}
	out string
}{
	// Bulk strings
	{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
	{in: "ohey", out: "$4\r\nohey\r\n"},
	{in: true, out: "$1\r\n1\r\n"},
	{in: false, out: "$1\r\n0\r\n"},
	{in: nil, out: "$-1\r\n"},
	{in: float32(5.5), out: "$3\r\n5.5\r\n"},
	{in: float64(5.5), out: "$3\r\n5.5\r\n"},
	{in: textCPMarshaler("ohey"), out: "$6\r\n_ohey_\r\n"},
	{in: binCPMarshaler("ohey"), out: "$6\r\n_ohey_\r\n"},

	// Int
	{in: 5, out: ":5\r\n"},
	{in: int64(5), out: ":5\r\n"},
	{in: uint64(5), out: ":5\r\n"},

	// Error
	{in: errors.New(":("), out: "-:(\r\n"},
}

func TestEncode(t *T) {
	buf := new(bytes.Buffer)
	e := NewEncoder(buf)
	for _, et := range encodeTests {
		require.Nil(t, e.Encode(et.in))
		assert.Equal(t, et.out, buf.String())
		buf.Reset()
	}
}

// TODO test Cmd
