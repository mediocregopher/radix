package radix

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	errors "golang.org/x/xerrors"
)

func closablePersistentPubSub() (PubSubConn, func()) {
	closeCh := make(chan chan bool)
	p := PersistentPubSub("", "", func(_, _ string) (Conn, error) {
		c := dial()
		go func() {
			closeRetCh := <-closeCh
			c.Close()
			closeRetCh <- true
		}()
		return c, nil
	})

	return p, func() {
		closeRetCh := make(chan bool)
		closeCh <- closeRetCh
		<-closeRetCh
	}
}

func TestPersistentPubSub(t *T) {
	p, closeFn := closablePersistentPubSub()
	pubCh := make(chan int)
	go func() {
		for i := 0; i < 1000; i++ {
			pubCh <- i
			if i%100 == 0 {
				time.Sleep(100 * time.Millisecond)
				closeFn()
				assert.Nil(t, p.Ping())
			}
		}
		close(pubCh)
	}()

	testSubscribe(t, p, pubCh)
}

func TestPersistentPubSubAbortAfter(t *T) {
	var errNope = errors.New("nope")
	var attempts int
	connFn := func(_, _ string) (Conn, error) {
		attempts++
		if attempts%3 != 0 {
			return nil, errNope
		}
		return dial(), nil
	}

	_, err := PersistentPubSubWithOpts("", "",
		PersistentPubSubConnFunc(connFn),
		PersistentPubSubAbortAfter(2))
	assert.Equal(t, errNope, err)

	attempts = 0
	p, err := PersistentPubSubWithOpts("", "",
		PersistentPubSubConnFunc(connFn),
		PersistentPubSubAbortAfter(3))
	assert.NoError(t, err)
	assert.NoError(t, p.Ping())
	p.Close()
}

// https://github.com/mediocregopher/radix/issues/184
func TestPersistentPubSubClose(t *T) {
	channel := "TestPersistentPubSubClose:" + randStr()

	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		pubConn := dial()
		for {
			err := pubConn.Do(Cmd(nil, "PUBLISH", channel, randStr()))
			assert.NoError(t, err)
			time.Sleep(10 * time.Millisecond)

			select {
			case <-stopCh:
				return
			default:
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		p := PersistentPubSub("", "", func(_, _ string) (Conn, error) {
			return dial(), nil
		})
		msgCh := make(chan PubSubMessage)
		p.Subscribe(msgCh, channel)
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
	channel := "TestPersistentPubSubUseAfterCloseDeadlock:" + randStr()

	p := PersistentPubSub("", "", func(_, _ string) (Conn, error) {
		return dial(), nil
	})
	msgCh := make(chan PubSubMessage)
	p.Subscribe(msgCh, channel)
	p.Close()

	errch := make(chan error)
	go func() {
		errch <- p.PUnsubscribe(msgCh, channel)
	}()

	select {
	case <-time.After(time.Second):
		assert.Fail(t, "PUnsubscribe call timeout")
	case err := <-errch:
		assert.Equal(t, err, ErrPersistentPubSubClosed)
	}

}
