package radix

import (
	"context"
	. "testing"
	"time"

	"errors"

	"github.com/stretchr/testify/assert"
)

func closablePersistentPubSub(t *T) (PubSubConn, func()) {
	ctx := testCtx(t)
	closeCh := make(chan chan bool)

	cfg := PersistentPubSubConfig{
		Dialer: Dialer{
			CustomConn: func(context.Context, string, string) (Conn, error) {
				c := dial()
				go func() {
					closeRetCh := <-closeCh
					c.Close()
					closeRetCh <- true
				}()
				return c, nil
			},
		},
	}

	p, err := cfg.New(ctx, nil)
	if err != nil {
		panic(err)
	}

	return p, func() {
		closeRetCh := make(chan bool)
		closeCh <- closeRetCh
		<-closeRetCh
	}
}

func TestPersistentPubSub(t *T) {
	ctx := testCtx(t)
	p, closeFn := closablePersistentPubSub(t)
	_ = closeFn
	_ = ctx
	pubCh := make(chan int)
	go func() {
		for i := 0; i < 1000; i++ {
			pubCh <- i
			if i%100 == 0 {
				time.Sleep(100 * time.Millisecond)
				closeFn()
				assert.Nil(t, p.Ping(ctx))
			}
		}
		close(pubCh)
	}()

	testSubscribe(t, p, pubCh)
	assert.NoError(t, p.Close())
}

func TestPersistentPubSubAbortAfter(t *T) {
	ctx := testCtx(t)
	var errNope = errors.New("nope")
	var attempts int
	connFn := func(_ context.Context, _, _ string) (Conn, error) {
		attempts++
		if attempts%3 != 0 {
			return nil, errNope
		}
		return dial(), nil
	}

	cfg := PersistentPubSubConfig{
		Dialer:     Dialer{CustomConn: connFn},
		AbortAfter: 2,
	}
	_, err := cfg.New(ctx, nil)
	assert.Equal(t, errNope, err)

	attempts = 0
	cfg.AbortAfter = 3
	p, err := cfg.New(ctx, nil)
	assert.NoError(t, err)
	assert.NoError(t, p.Ping(ctx))
	assert.NoError(t, p.Close())
}

// https://github.com/mediocregopher/radix/issues/184
func TestPersistentPubSubClose(t *T) {
	ctx := testCtx(t)
	channel := "TestPersistentPubSubClose:" + randStr()

	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
		<-stopCh
	}()
	go func() {
		pubConn := dial()
		defer pubConn.Close()
		for {
			err := pubConn.Do(ctx, Cmd(nil, "PUBLISH", channel, randStr()))
			assert.NoError(t, err)
			select {
			case <-stopCh:
				stopCh <- struct{}{}
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		cfg := PersistentPubSubConfig{Dialer: dialer}

		p, err := cfg.New(ctx, nil)
		if err != nil {
			panic(err)
		}
		msgCh := make(chan PubSubMessage)
		p.Subscribe(ctx, msgCh, channel)
		// drain msgCh till it closes
		go func() {
			for range msgCh {
			}
		}()
		p.Close()
		close(msgCh)
	}
}

func TestPersistentPubSubUseAfterCloseDeadlock(t *T) {
	ctx := testCtx(t)
	channel := "TestPersistentPubSubUseAfterCloseDeadlock:" + randStr()

	cfg := PersistentPubSubConfig{Dialer: dialer}
	p, err := cfg.New(ctx, nil)
	if err != nil {
		panic(err)
	}
	msgCh := make(chan PubSubMessage)
	p.Subscribe(ctx, msgCh, channel)
	p.Close()

	errch := make(chan error)
	go func() {
		errch <- p.PUnsubscribe(ctx, msgCh, channel)
	}()

	select {
	case <-time.After(time.Second):
		assert.Fail(t, "PUnsubscribe call timeout")
	case err := <-errch:
		assert.Error(t, err)
	}
}
