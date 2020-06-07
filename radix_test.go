package radix

import (
	"crypto/rand"
	"encoding/hex"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func dial(opts ...DialOpt) Conn {
	c, err := Dial("tcp", "127.0.0.1:6379", opts...)
	if err != nil {
		panic(err)
	}
	return c
}
