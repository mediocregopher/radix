package radix

import (
	"fmt"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Watching the watchmen

func TestStub(t *T) {
	getSetStub := func() Conn {
		m := map[string]string{}
		fn := func(args []string) interface{} {
			switch args[0] {
			case "GET":
				return m[args[1]]
			case "SET":
				m[args[1]] = args[2]
				return nil
			default:
				return fmt.Errorf("getSet doesn't support command %q", args[0])
			}
		}
		return Stub("tcp", "127.0.0.1:6379", fn)
	}

	stub := getSetStub()

	{
		var foo string
		require.Nil(t, Cmd("SET", "foo", "a").Run(stub))
		require.Nil(t, Cmd("GET", "foo").Into(&foo).Run(stub))
		assert.Equal(t, "a", foo)
	}

	{
		var foo int
		require.Nil(t, Cmd("SET", "foo", 1).Run(stub))
		require.Nil(t, Cmd("GET", "foo").Into(&foo).Run(stub))
		assert.Equal(t, 1, foo)
	}
}
