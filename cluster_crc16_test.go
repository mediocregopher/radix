package radix

import (
	"fmt"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterSlot(t *T) {
	// basic test
	assert.Equal(t, uint16(0x31c3), ClusterSlot([]byte("123456789")))

	// this is more to test that the hash tag checking works than anything
	k := []byte(randStr())
	crcSlot := ClusterSlot(k)

	kk := randStr()
	assert.Equal(t, crcSlot, ClusterSlot([]byte(fmt.Sprintf("{%s}%s", k, kk))))
	assert.Equal(t, crcSlot, ClusterSlot([]byte(fmt.Sprintf("{%s}}%s", k, kk))))
	assert.Equal(t, crcSlot, ClusterSlot([]byte(fmt.Sprintf("%s{%s}", kk, k))))
	assert.Equal(t, crcSlot, ClusterSlot([]byte(fmt.Sprintf("%s{%s}}%s", kk, k, kk))))
}
