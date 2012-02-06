package redis

import (
	"fmt"
	"strings"
)

type command struct {
	cmd  string
	args []interface{}
}

//* Interfaces

// Hashable represents types for Redis hashes.
type Hashable interface {
	GetHash() Hash
	SetHash(h Hash)
}

//* Useful helpers

// valueToBytes converts a value into a byte slice.
func valueToBytes(v interface{}) []byte {
	var bs []byte

	switch vt := v.(type) {
	case string:
		bs = []byte(vt)
	case []byte:
		bs = vt
	case []string:
		bs = []byte(strings.Join(vt, "\r\n"))
	case map[string]string:
		tmp := make([]string, len(vt))
		i := 0

		for vtk, vtv := range vt {
			tmp[i] = fmt.Sprintf("%v:%v", vtk, vtv)

			i++
		}

		bs = []byte(strings.Join(tmp, "\r\n"))
	case bool:
		if vt {
			bs = []byte("1")
		} else {
			bs = []byte("0")
		}
	default:
		bs = []byte(fmt.Sprintf("%v", vt))
	}

	return bs
}
