package resp3

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"github.com/mediocregopher/radix/v4/resp"
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
			o := resp.NewOpts()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sr.Reset(input)
				br.Reset(&sr)

				var n Number
				if err := n.UnmarshalRESP(br, o); err != nil {
					b.Fatalf("failed to unmarshal %q: %s", input, err)
				}
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	b.Run("Map", func(b *testing.B) {
		b.ReportAllocs()

		const input = "*8\r\n" +
			"$3\r\nFoo\r\n" + "$1\r\n1\r\n" +
			"$3\r\nBAZ\r\n" + "$2\r\n22\r\n" +
			"$3\r\nBoz\r\n" + "$4\r\n4444\r\n" +
			"$3\r\nBiz\r\n" + "$8\r\n88888888\r\n"

		var sr strings.Reader
		br := bufio.NewReader(&sr)
		o := resp.NewOpts()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sr.Reset(input)
			br.Reset(&sr)

			var m map[string]string
			if err := Unmarshal(br, &m, o); err != nil {
				b.Fatalf("failed to unmarshal %q: %s", input, err)
			}
		}
	})

	b.Run("Struct", func(b *testing.B) {
		b.ReportAllocs()

		const input = "*8\r\n" +
			"$3\r\nFoo\r\n" + ":1\r\n" +
			"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
			"$3\r\nBoz\r\n" + ":5\r\n" +
			"$3\r\nBiz\r\n" + "$2\r\n10\r\n"

		var sr strings.Reader
		br := bufio.NewReader(&sr)
		o := resp.NewOpts()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sr.Reset(input)
			br.Reset(&sr)

			var s testStructA
			if err := Unmarshal(br, &s, o); err != nil {
				b.Fatalf("failed to unmarshal %q: %s", input, err)
			}
		}
	})
}
