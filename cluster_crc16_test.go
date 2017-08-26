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

	// ClusterSlot without handling curly braces
	rawClusterSlot := func(s string) uint16 {
		return CRC16([]byte(s)) % numSlots
	}

	clusterSlotf := func(s string, args ...interface{}) uint16 {
		return ClusterSlot([]byte(fmt.Sprintf(s, args...)))
	}

	kk := randStr()
	assert.Equal(t, crcSlot, clusterSlotf("{%s}%s", k, kk))
	assert.Equal(t, crcSlot, clusterSlotf("{%s}}%s", k, kk))
	assert.Equal(t, crcSlot, clusterSlotf("%s{%s}", kk, k))
	assert.Equal(t, crcSlot, clusterSlotf("%s{%s}}%s", kk, k, kk))
	assert.Equal(t, rawClusterSlot(string(k)+"{"), clusterSlotf("%s{", k))
	// if the braces are empty it should match the whole string
	assert.Equal(t, rawClusterSlot("foo{}{bar}"), ClusterSlot([]byte(`foo{}{bar}`)))
}
