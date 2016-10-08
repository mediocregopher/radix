package cluster

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterInit(t *T) {
	scl := newStubCluster(testTopo)
	c, err := newCluster(scl.poolFunc(), scl.addrs()...)
	require.Nil(t, err)

	assert.Len(t, c.pools, len(testTopo))
}
