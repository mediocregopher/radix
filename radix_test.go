package radix

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"sync"
	. "testing"
	"time"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

var flagRESP3 = flag.Bool("resp3", false, "Enables RESP3 for all tests")

func dial() Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	protocol := ""
	if *flagRESP3 {
		protocol = "3"
	}
	c, err := Dialer{Protocol: protocol}.Dial(ctx, "tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	return c
}

var dialer = Dialer{
	CustomConn: func(context.Context, string, string) (Conn, error) {
		return dial(), nil
	},
}

var (
	testCtxs  = map[TB]context.Context{}
	testCtxsL sync.Mutex
)

func testCtx(tb TB) context.Context {
	tb.Helper()

	testCtxsL.Lock()
	defer testCtxsL.Unlock()

	if ctx, ok := testCtxs[tb]; ok {
		return ctx
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	tb.Cleanup(cancel)
	testCtxs[tb] = ctx
	return ctx
}
