package radix

import (
	"bufio"
	"bytes"
	. "testing"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func respArr(ii ...interface{}) resp.Marshaler {
	var ar resp2.Array
	for _, i := range ii {
		if m, ok := i.(resp.Marshaler); ok {
			ar.A = append(ar.A, m)
		} else {
			ar.A = append(ar.A, resp2.Any{I: i})
		}
	}
	return ar
}

var testTopoResp = func() resp.Marshaler {
	return respArr(
		respArr(13653, 16383,
			respArr("10.128.0.30", 6379, "f7e95c8730634159bc79f9edac566f7b1c964cdd"),
			respArr("10.128.0.27", 6379), // older redis instances don't return id
		),
		respArr(5461, 8190,
			respArr("10.128.0.36", 6379, "a3c69665bb05c8d5770407cad5b35af29e740586"),
			respArr("10.128.0.24", 6379, "bef29809fbfe964d3b7c3ad02d3d9a40e55de317"),
		),
		respArr(10923, 13652,
			respArr("10.128.0.20", 6379, "e0abc57f65496368e73a9b52b55efd00668adab7"),
			respArr("10.128.0.35", 6379, "3e231d265d6ec0c5aa11614eb86704b65f7f909e"),
		),
		respArr(8191, 10922,
			respArr("10.128.0.29", 6379, "43f1b46d2772fd7bb78b144ddfc3fe77a9f21748"),
			respArr("10.128.0.26", 6379, "25339aee29100492d73cbfb1518e318ce1f2fd57"),
		),
		respArr(2730, 5460,
			respArr("10.128.0.25", 6379, "78e4bb43f68cdc929815a65b4db0697fdda2a9fa"),
			respArr("10.128.0.28", 6379, "5a57538cd8ae102daee1dd7f34070e133ff92173"),
		),
		respArr(0, 2729,
			respArr("10.128.0.34", 6379, "062d8ca98db4deb6b2a3fc776a774dbb710c1a24"),
			respArr("10.128.0.3", 6379, "7be2403f92c00d4907da742ffa4c84b935228350"),
		),
	)
}()

var testTopo = func() ClusterTopo {
	buf := new(bytes.Buffer)
	if err := testTopoResp.MarshalRESP(buf); err != nil {
		panic(err)
	}
	var tt ClusterTopo
	if err := tt.UnmarshalRESP(bufio.NewReader(buf)); err != nil {
		panic(err)
	}
	return tt
}()

func TestClusterTopo(t *T) {
	testTopoExp := ClusterTopo{
		ClusterNode{
			Slots: [][2]uint16{{0, 2730}},
			Addr:  "10.128.0.34:6379", ID: "062d8ca98db4deb6b2a3fc776a774dbb710c1a24",
		},
		ClusterNode{
			Slots: [][2]uint16{{0, 2730}},
			Addr:  "10.128.0.3:6379", ID: "7be2403f92c00d4907da742ffa4c84b935228350",
			SecondaryOfAddr: "10.128.0.34:6379",
			SecondaryOfID:   "062d8ca98db4deb6b2a3fc776a774dbb710c1a24",
		},

		ClusterNode{
			Slots: [][2]uint16{{2730, 5461}},
			Addr:  "10.128.0.25:6379", ID: "78e4bb43f68cdc929815a65b4db0697fdda2a9fa",
		},
		ClusterNode{
			Slots: [][2]uint16{{2730, 5461}},
			Addr:  "10.128.0.28:6379", ID: "5a57538cd8ae102daee1dd7f34070e133ff92173",
			SecondaryOfAddr: "10.128.0.25:6379",
			SecondaryOfID:   "78e4bb43f68cdc929815a65b4db0697fdda2a9fa",
		},

		ClusterNode{
			Slots: [][2]uint16{{5461, 8191}},
			Addr:  "10.128.0.36:6379", ID: "a3c69665bb05c8d5770407cad5b35af29e740586",
		},
		ClusterNode{
			Slots: [][2]uint16{{5461, 8191}},
			Addr:  "10.128.0.24:6379", ID: "bef29809fbfe964d3b7c3ad02d3d9a40e55de317",
			SecondaryOfAddr: "10.128.0.36:6379",
			SecondaryOfID:   "a3c69665bb05c8d5770407cad5b35af29e740586",
		},

		ClusterNode{
			Slots: [][2]uint16{{8191, 10923}},
			Addr:  "10.128.0.29:6379", ID: "43f1b46d2772fd7bb78b144ddfc3fe77a9f21748",
		},
		ClusterNode{
			Slots: [][2]uint16{{8191, 10923}},
			Addr:  "10.128.0.26:6379", ID: "25339aee29100492d73cbfb1518e318ce1f2fd57",
			SecondaryOfAddr: "10.128.0.29:6379",
			SecondaryOfID:   "43f1b46d2772fd7bb78b144ddfc3fe77a9f21748",
		},

		ClusterNode{
			Slots: [][2]uint16{{10923, 13653}},
			Addr:  "10.128.0.20:6379", ID: "e0abc57f65496368e73a9b52b55efd00668adab7",
		},
		ClusterNode{
			Slots: [][2]uint16{{10923, 13653}},
			Addr:  "10.128.0.35:6379", ID: "3e231d265d6ec0c5aa11614eb86704b65f7f909e",
			SecondaryOfAddr: "10.128.0.20:6379",
			SecondaryOfID:   "e0abc57f65496368e73a9b52b55efd00668adab7",
		},

		ClusterNode{
			Slots: [][2]uint16{{13653, 16384}},
			Addr:  "10.128.0.30:6379", ID: "f7e95c8730634159bc79f9edac566f7b1c964cdd",
		},
		ClusterNode{
			Slots: [][2]uint16{{13653, 16384}},
			Addr:  "10.128.0.27:6379", ID: "",
			SecondaryOfAddr: "10.128.0.30:6379",
			SecondaryOfID:   "f7e95c8730634159bc79f9edac566f7b1c964cdd",
		},
	}

	// make sure, to start with, the testTopo matches what we expect it to
	assert.Equal(t, testTopoExp, testTopo)

	// Make sure both Marshal/UnmarshalRESP on it are working correctly (the
	// calls in testTopoResp aren't actually on Topo's methods)
	buf := new(bytes.Buffer)
	require.Nil(t, testTopo.MarshalRESP(buf))
	var testTopo2 ClusterTopo
	require.Nil(t, testTopo2.UnmarshalRESP(bufio.NewReader(buf)))
	assert.Equal(t, testTopoExp, testTopo2)
}

// Test parsing a topology where a node in the cluster has two different sets of
// slots, as well as a secondary
func TestClusterTopoSplitSlots(t *T) {
	clusterSlotsResp := respArr(
		respArr(0, 0,
			respArr("127.0.0.1", "7001", "90900dd4ef2182825bc853c448737b2ba9975a50"),
			respArr("127.0.0.1", "7011", "073a013f8886b6cf4c1b018612102601534912e9"),
		),
		respArr(1, 8191,
			respArr("127.0.0.1", "7000", "3ff1ddc420cfceeb4c42dc4b1f8f85c3acf984fe"),
		),
		respArr(8192, 16383,
			respArr("127.0.0.1", "7001", "90900dd4ef2182825bc853c448737b2ba9975a50"),
			respArr("127.0.0.1", "7011", "073a013f8886b6cf4c1b018612102601534912e9"),
		),
	)
	expTopo := ClusterTopo{
		ClusterNode{
			Slots: [][2]uint16{{0, 1}, {8192, 16384}},
			Addr:  "127.0.0.1:7001", ID: "90900dd4ef2182825bc853c448737b2ba9975a50",
		},
		ClusterNode{
			Slots: [][2]uint16{{0, 1}, {8192, 16384}},
			Addr:  "127.0.0.1:7011", ID: "073a013f8886b6cf4c1b018612102601534912e9",
			SecondaryOfAddr: "127.0.0.1:7001",
			SecondaryOfID:   "90900dd4ef2182825bc853c448737b2ba9975a50",
		},
		ClusterNode{
			Slots: [][2]uint16{{1, 8192}},
			Addr:  "127.0.0.1:7000", ID: "3ff1ddc420cfceeb4c42dc4b1f8f85c3acf984fe",
		},
	}

	// unmarshal the resp into a Topo and make sure it matches expTopo
	{
		buf := new(bytes.Buffer)
		require.Nil(t, clusterSlotsResp.MarshalRESP(buf))
		var topo ClusterTopo
		require.Nil(t, topo.UnmarshalRESP(bufio.NewReader(buf)))
		assert.Equal(t, expTopo, topo)
	}

	// marshal Topo, then re-unmarshal, and make sure it still matches
	{
		buf := new(bytes.Buffer)
		require.Nil(t, expTopo.MarshalRESP(buf))
		var topo ClusterTopo
		require.Nil(t, topo.UnmarshalRESP(bufio.NewReader(buf)))
		assert.Equal(t, expTopo, topo)
	}

}
