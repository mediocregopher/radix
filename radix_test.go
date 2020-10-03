package radix

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

func dial(opts ...DialOpt) Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, "tcp", "127.0.0.1:6379", opts...)
	if err != nil {
		panic(err)
	}
	return c
}

var (
	testCtxs  = map[TB]context.Context{}
	testCtxsL sync.Mutex
)

func testCtx(t TB) context.Context {
	testCtxsL.Lock()
	defer testCtxsL.Unlock()

	if ctx, ok := testCtxs[t]; ok {
		return ctx
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	testCtxs[t] = ctx
	return ctx
}
