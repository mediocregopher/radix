package radix

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPersistentPubSub(t *T) {
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
