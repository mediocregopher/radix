package radix

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"
	"time"

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

func TestCloseBehavior(t *T) {
	c := dial()

	// sanity check
	var out string
	require.Nil(t, CmdNoKey(&out, "ECHO", "foo").Run(c))
	assert.Equal(t, "foo", out)

	c.Close()
	require.NotNil(t, CmdNoKey(&out, "ECHO", "foo").Run(c))
	require.NotNil(t, c.NetConn().SetDeadline(time.Now()))
}
