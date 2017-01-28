package pubsub

import (
	. "testing"
	"time"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/stretchr/testify/assert"
)

func TestPersistent(t *T) {
	closeCh := make(chan chan bool)
	p := NewPersistent(func() (radix.Conn, error) {
		c := testConn(t)
		go func() {
			closeRetCh := <-closeCh
			c.Close()
			closeRetCh <- true
		}()
		return c, nil
	})

	pubCh := make(chan int)
	go func() {
		for i := 0; i < 1000; i++ {
			pubCh <- i
			if i%100 == 0 {
				time.Sleep(100 * time.Millisecond)
				closeRetCh := make(chan bool)
				closeCh <- closeRetCh
				<-closeRetCh
				assert.Nil(t, p.Ping())
			}
		}
		close(pubCh)
	}()

	testSubscribe(t, p, pubCh)
}
