package radix

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dial() Conn {
	c, err := Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	return c
}

func TestPipeline(t *T) {
	c := dial()
	// Do this multiple times to make sure pipeline resetting happens correctly
	for i := 0; i < 10; i++ {
		ss := []string{
			testutil.RandStr(),
			testutil.RandStr(),
			testutil.RandStr(),
		}
		rr := Pipeline(c,
			NewCmd("ECHO", ss[0]),
			NewCmd("ECHO", ss[1]),
			NewCmd("ECHO", ss[2]),
		)

		for i := range ss {
			exp := ss[i]
			out, err := rr[i].Str()
			require.Nil(t, err)
			assert.Equal(t, exp, out)
		}
	}
}
