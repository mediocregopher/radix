package resp

import (
	"bufio"
	"bytes"
	"fmt"
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
	}{
		{"1"},
		{"1.23"},
		{"-1"},
		{"-1.23"},
		{"+1"},
		{"+1.23"},
	}

	for _, test := range tests {
		input := test.In

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var r strings.Reader

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				bfloat, _ = readFloat(&r, 64)
			}
		})
	}
}

var bint int64

func BenchmarkReadInt(b *testing.B) {
	tests := []struct {
		In string
	}{
		{"1"},
		{"123"},
		{"-1"},
		{"-123"},
		{"+1"},
		{"+123"},
	}

	for _, test := range tests {
		input := test.In

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var r strings.Reader

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				bint, _ = readInt(&r)
			}
		})
	}
}

var buint uint64

func BenchmarkReadUint(b *testing.B) {
	tests := []struct {
		In string
	}{
		{"1"},
		{"123"},
	}

	for _, test := range tests {
		input := test.In

		b.Run(fmt.Sprint(test.In), func(b *testing.B) {
			var r strings.Reader

			for i := 0; i < b.N; i++ {
				r.Reset(input)
				buint, _ = readUint(&r)
			}
		})
	}
}

func BenchmarkReadAllAppend(b *testing.B) {
	respBytes := []byte("$5\r\nhello\r\n")

	for _, bcap := range []int{0, len(respBytes), len(respBytes) + bytes.MinRead} {
		b.Run("Capacity"+strconv.Itoa(bcap), func(b *testing.B) {
			var br bytes.Reader
			buf := make([]byte, 0, bcap)

			for i := 0; i < b.N; i++ {
				br.Reset(respBytes)

				if _, err := readAllAppend(&br, buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
