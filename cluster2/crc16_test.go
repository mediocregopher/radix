package cluster

import (
	"fmt"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestSlot(t *T) {
	// basic test
	assert.Equal(t, uint16(0x31c3), Slot([]byte("123456789")))

	// this is more to test that the hash tag checking works than anything
	k := []byte(randStr())
	crcSlot := Slot(k)

	kk := randStr()
	assert.Equal(t, crcSlot, Slot([]byte(fmt.Sprintf("{%s}%s", k, kk))))
	assert.Equal(t, crcSlot, Slot([]byte(fmt.Sprintf("{%s}}%s", k, kk))))
	assert.Equal(t, crcSlot, Slot([]byte(fmt.Sprintf("%s{%s}", kk, k))))
	assert.Equal(t, crcSlot, Slot([]byte(fmt.Sprintf("%s{%s}}%s", kk, k, kk))))
}
