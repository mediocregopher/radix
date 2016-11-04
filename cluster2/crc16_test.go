package cluster

import (
	"fmt"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestCRC16(t *T) {
}

func TestSlot(t *T) {
	// basic test
	assert.Equal(t, uint16(0x31c3), Slot("123456789"))

	// this is more to test that the hash tag checking works than anything
	k := randStr()
	crcSlot := Slot(k)

	kk := randStr()
	assert.Equal(t, crcSlot, Slot(fmt.Sprintf("{%s}%s", k, kk)))
	assert.Equal(t, crcSlot, Slot(fmt.Sprintf("{%s}}%s", k, kk)))
	assert.Equal(t, crcSlot, Slot(fmt.Sprintf("%s{%s}", kk, k)))
	assert.Equal(t, crcSlot, Slot(fmt.Sprintf("%s{%s}}%s", kk, k, kk)))
}
