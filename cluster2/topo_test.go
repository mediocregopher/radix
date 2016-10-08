package cluster

import (
	. "testing"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/stretchr/testify/assert"
)

var testTopoResp = func() radix.Resp {
	type respArr []interface{}
	return radix.NewResp(respArr{
		respArr{13653, 16383,
			respArr{"10.128.0.30", 6379, "f7e95c8730634159bc79f9edac566f7b1c964cdd"},
			respArr{"10.128.0.27", 6379}, // older redis instances don't return id
		},
		respArr{5461, 8190,
			respArr{"10.128.0.36", 6379, "a3c69665bb05c8d5770407cad5b35af29e740586"},
			respArr{"10.128.0.24", 6379, "bef29809fbfe964d3b7c3ad02d3d9a40e55de317"},
		},
		respArr{10923, 13652,
			respArr{"10.128.0.20", 6379, "e0abc57f65496368e73a9b52b55efd00668adab7"},
			respArr{"10.128.0.35", 6379, "3e231d265d6ec0c5aa11614eb86704b65f7f909e"},
		},
		respArr{8191, 10922,
			respArr{"10.128.0.29", 6379, "43f1b46d2772fd7bb78b144ddfc3fe77a9f21748"},
			respArr{"10.128.0.26", 6379, "25339aee29100492d73cbfb1518e318ce1f2fd57"},
		},
		respArr{2730, 5460,
			respArr{"10.128.0.25", 6379, "78e4bb43f68cdc929815a65b4db0697fdda2a9fa"},
			respArr{"10.128.0.28", 6379, "5a57538cd8ae102daee1dd7f34070e133ff92173"},
		},
		respArr{0, 2729,
			respArr{"10.128.0.34", 6379, "062d8ca98db4deb6b2a3fc776a774dbb710c1a24"},
			respArr{"10.128.0.3", 6379, "7be2403f92c00d4907da742ffa4c84b935228350"},
		},
	})
}()

var testTopo = func() topo {
	tt, err := parseTopo(testTopoResp)
	if err != nil {
		panic(err)
	}
	return tt
}()

func TestParseTopo(t *T) {
	testTopoExp := topo{
		topoNode{
			slots: [2]uint16{0, 2730},
			addr:  "10.128.0.34:6379", id: "062d8ca98db4deb6b2a3fc776a774dbb710c1a24",
		},
		topoNode{
			slots: [2]uint16{0, 2730},
			addr:  "10.128.0.3:6379", id: "7be2403f92c00d4907da742ffa4c84b935228350",
			slave: true,
		},
		topoNode{
			slots: [2]uint16{2730, 5461},
			addr:  "10.128.0.25:6379", id: "78e4bb43f68cdc929815a65b4db0697fdda2a9fa",
		},
		topoNode{
			slots: [2]uint16{2730, 5461},
			addr:  "10.128.0.28:6379", id: "5a57538cd8ae102daee1dd7f34070e133ff92173",
			slave: true,
		},
		topoNode{
			slots: [2]uint16{5461, 8191},
			addr:  "10.128.0.36:6379", id: "a3c69665bb05c8d5770407cad5b35af29e740586",
		},
		topoNode{
			slots: [2]uint16{5461, 8191},
			addr:  "10.128.0.24:6379", id: "bef29809fbfe964d3b7c3ad02d3d9a40e55de317",
			slave: true,
		},
		topoNode{
			slots: [2]uint16{8191, 10923},
			addr:  "10.128.0.29:6379", id: "43f1b46d2772fd7bb78b144ddfc3fe77a9f21748",
		},
		topoNode{
			slots: [2]uint16{8191, 10923},
			addr:  "10.128.0.26:6379", id: "25339aee29100492d73cbfb1518e318ce1f2fd57",
			slave: true,
		},
		topoNode{
			slots: [2]uint16{10923, 13653},
			addr:  "10.128.0.20:6379", id: "e0abc57f65496368e73a9b52b55efd00668adab7",
		},
		topoNode{
			slots: [2]uint16{10923, 13653},
			addr:  "10.128.0.35:6379", id: "3e231d265d6ec0c5aa11614eb86704b65f7f909e",
			slave: true,
		},
		topoNode{
			slots: [2]uint16{13653, 16384},
			addr:  "10.128.0.30:6379", id: "f7e95c8730634159bc79f9edac566f7b1c964cdd",
		},
		topoNode{
			slots: [2]uint16{13653, 16384},
			addr:  "10.128.0.27:6379", id: "", slave: true,
		},
	}

	assert.Equal(t, testTopoExp, testTopo)
}
