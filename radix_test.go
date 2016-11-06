package radix

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

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
		out := make([]string, len(ss))
		p := Pipeline{Conn: c}
		for i := range ss {
			p.Append(&out[i], Cmd{}.C("ECHO").A(ss[i]))
		}
		require.Nil(t, p.Run())

		for i := range ss {
			assert.Equal(t, ss[i], out[i])
		}
	}
}
