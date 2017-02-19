package cluster

import (
	"bufio"
	"bytes"
	. "testing"

	"github.com/mediocregopher/radix.v2/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTopoResp = func() resp.Marshaler {
	respArr := func(ii ...interface{}) resp.Marshaler {
		var ar resp.Array
		for _, i := range ii {
			if m, ok := i.(resp.Marshaler); ok {
				ar.A = append(ar.A, m)
			} else {
				ar.A = append(ar.A, resp.Any{I: i})
			}
		}
		return ar
	}
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

var testTopo = func() Topo {
	buf := new(bytes.Buffer)
	if err := testTopoResp.MarshalRESP(nil, buf); err != nil {
		panic(err)
	}
	var tt Topo
	if err := tt.UnmarshalRESP(nil, bufio.NewReader(buf)); err != nil {
		panic(err)
	}
	return tt
}()

func TestParseTopo(t *T) {
	testTopoExp := Topo{
		Node{
			Slots: [2]uint16{0, 2730},
			Addr:  "10.128.0.34:6379", ID: "062d8ca98db4deb6b2a3fc776a774dbb710c1a24",
		},
		Node{
			Slots: [2]uint16{0, 2730},
			Addr:  "10.128.0.3:6379", ID: "7be2403f92c00d4907da742ffa4c84b935228350",
			Slave: true,
		},
		Node{
			Slots: [2]uint16{2730, 5461},
			Addr:  "10.128.0.25:6379", ID: "78e4bb43f68cdc929815a65b4db0697fdda2a9fa",
		},
		Node{
			Slots: [2]uint16{2730, 5461},
			Addr:  "10.128.0.28:6379", ID: "5a57538cd8ae102daee1dd7f34070e133ff92173",
			Slave: true,
		},
		Node{
			Slots: [2]uint16{5461, 8191},
			Addr:  "10.128.0.36:6379", ID: "a3c69665bb05c8d5770407cad5b35af29e740586",
		},
		Node{
			Slots: [2]uint16{5461, 8191},
			Addr:  "10.128.0.24:6379", ID: "bef29809fbfe964d3b7c3ad02d3d9a40e55de317",
			Slave: true,
		},
		Node{
			Slots: [2]uint16{8191, 10923},
			Addr:  "10.128.0.29:6379", ID: "43f1b46d2772fd7bb78b144ddfc3fe77a9f21748",
		},
		Node{
			Slots: [2]uint16{8191, 10923},
			Addr:  "10.128.0.26:6379", ID: "25339aee29100492d73cbfb1518e318ce1f2fd57",
			Slave: true,
		},
		Node{
			Slots: [2]uint16{10923, 13653},
			Addr:  "10.128.0.20:6379", ID: "e0abc57f65496368e73a9b52b55efd00668adab7",
		},
		Node{
			Slots: [2]uint16{10923, 13653},
			Addr:  "10.128.0.35:6379", ID: "3e231d265d6ec0c5aa11614eb86704b65f7f909e",
			Slave: true,
		},
		Node{
			Slots: [2]uint16{13653, 16384},
			Addr:  "10.128.0.30:6379", ID: "f7e95c8730634159bc79f9edac566f7b1c964cdd",
		},
		Node{
			Slots: [2]uint16{13653, 16384},
			Addr:  "10.128.0.27:6379", ID: "", Slave: true,
		},
	}

	// make sure, to start with, the testTopo matches what we expect it to
	assert.Equal(t, testTopoExp, testTopo)

	// Make sure both Marshal/UnmarshalRESP on it are working correctly (the
	// calls in testTopoResp aren't actually on Topo's methods)
	buf := new(bytes.Buffer)
	require.Nil(t, testTopo.MarshalRESP(nil, buf))
	var testTopo2 Topo
	require.Nil(t, testTopo2.UnmarshalRESP(nil, bufio.NewReader(buf)))
	assert.Equal(t, testTopoExp, testTopo2)
}
