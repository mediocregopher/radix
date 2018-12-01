package resp2

import (
	"bufio"
	"fmt"
	"github.com/mediocregopher/radix/v3/internal/bytesutil"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkIntUnmarshalRESP(b *testing.B) {
	tests := []struct {
		In string
	}{
		{"-1"},
		{"-123"},
		{"1"},
		{"123"},
		{"+1"},
		{"+123"},
	}

	for _, test := range tests {
		input := ":" + test.In + "\r\n"

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var sr strings.Reader
			br := bufio.NewReader(&sr)

			for i := 0; i < b.N; i++ {
				sr.Reset(input)
				br.Reset(&sr)

				var i Int
				if err := i.UnmarshalRESP(br); err != nil {
					b.Fatalf("failed to unmarshal %q: %s", input, err)
				}
			}
		})
	}
}

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

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				bfloat, _ = bytesutil.ReadFloat(&r, 64, n)
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

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				bint, _ = bytesutil.ReadInt(&r, n)
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

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				buint, _ = bytesutil.ReadUint(&r, n)
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
			buf := *bytesutil.GetBytes()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := bytesutil.ReadNAppend(&r, buf, n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
