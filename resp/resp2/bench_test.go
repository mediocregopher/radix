package resp2

import (
	"bufio"
	"fmt"
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
