package cluster

import (
	. "testing"
)

func TestCRC16(t *T) {
	if c := CRC16([]byte("123456789")); c != 0x31c3 {
		t.Fatalf("checksum came out to %x not %x", c, 0x31c3)
	}
}
