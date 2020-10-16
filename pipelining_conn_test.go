package radix

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type pipeliningConnInner struct {
	Conn
	encDecCalls int
}

func (pci *pipeliningConnInner) EncodeDecode(ctx context.Context, m, u interface{}) error {
	pci.encDecCalls++
	err := pci.Conn.EncodeDecode(ctx, m, u)
	return err
}

func TestPipeliningConn(t *testing.T) {
	t.Skip("not sure if any of this is getting kept anyway")
	ctx := testCtx(t)
	const concurrent = 10

	timerCh := make(chan time.Time)
	connInner := &pipeliningConnInner{Conn: dial()}
	pc := NewPipeliningConn(connInner,
		PipeliningConnBatchSize(concurrent))
	pc.(*pipeliningConn).testTimerCh = timerCh

	doneCh := make(chan struct{}, concurrent)
	do := func() {
		in := randStr()
		var out string
		err := pc.Do(ctx, Cmd(&out, "ECHO", in))
		assert.NoError(t, err)
		assert.Equal(t, in, out)
		doneCh <- struct{}{}
	}

	for i := 0; i < concurrent-1; i++ {
		go do()
	}

	// until a final do is called to fill the batch nothing should happen
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 0, len(doneCh))
	assert.Equal(t, 0, connInner.encDecCalls)

	go do()
	for i := 0; i < concurrent; i++ {
		select {
		case <-doneCh:
		case <-time.After(1 * time.Second):
			t.Fatal("waited too long for doneCh to be written to")
		}
	}
	assert.Equal(t, 0, len(doneCh))
	assert.Equal(t, 1, connInner.encDecCalls)

	// only spawn half the number of concurrent, but we'll trigger the timer so
	// they still get flushed.
	for i := 0; i < concurrent/2; i++ {
		go do()
	}

	// nothing should happen yet
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 0, len(doneCh))
	assert.Equal(t, 1, connInner.encDecCalls)

	timerCh <- time.Now() // kick off the flush
	for i := 0; i < concurrent/2; i++ {
		select {
		case <-doneCh:
		case <-time.After(1 * time.Second):
			t.Fatal("waited too long for doneCh to be written to")
		}
	}
	assert.Equal(t, 0, len(doneCh))
	assert.Equal(t, 2, connInner.encDecCalls)
}
