package bytesutil

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var bfloat float64

func BenchmarkReadFloat(b *testing.B) {
	tests := []struct {
		In string
		N  int
	}{
		{"1", 1},
		{"1.23", 4},
		{"-1", 2},
		{"-1.23", 5},
		{"+1", 2},
		{"+1.23", 5},
	}

	for _, test := range tests {
		input, n := test.In, test.N

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var r strings.Reader
			var scratch []byte

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				scratch = scratch[:0]
				bfloat, _ = ReadFloat(&r, 64, n, &scratch)
			}
		})
	}
}

var bint int64

func BenchmarkReadInt(b *testing.B) {
	tests := []struct {
		In string
		N  int
	}{
		{"1", 1},
		{"123", 3},
		{"-1", 2},
		{"-123", 4},
		{"+1", 2},
		{"+123", 4},
	}

	for _, test := range tests {
		input, n := test.In, test.N

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var r strings.Reader
			var scratch []byte

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				scratch = scratch[:0]
				bint, _ = ReadInt(&r, n, &scratch)
			}
		})
	}
}

var buint uint64

func BenchmarkReadUint(b *testing.B) {
	tests := []struct {
		In string
		N  int
	}{
		{"1", 1},
		{"123", 123},
	}

	for _, test := range tests {
		input, n := test.In, test.N

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var r strings.Reader
			var scratch []byte

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				scratch = scratch[:0]
				buint, _ = ReadUint(&r, n, &scratch)
			}
		})
	}
}

type nothingReader struct{}

func (nothingReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}

func BenchmarkReadNAppend(b *testing.B) {
	for _, n := range []int{0, 64, 512, 4096} {
		b.Run("N="+strconv.Itoa(n), func(b *testing.B) {
			var r nothingReader
			var scratch []byte

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				scratch = scratch[:0]
				if _, err := ReadNAppend(&r, scratch, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
