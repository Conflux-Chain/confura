package util

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
)

// Follow Conflux eSpace's LogFilterParams::matches, including missing wildcard
// positions: https://github.com/Conflux-Chain/conflux-rust/blob/2cb16583060d7a569271adb59cc1a9cd92dfd447/crates/primitives/src/filter.rs#L317-L339
func TestEthLogTopicsMatchConfluxSemantics(t *testing.T) {
	a, b := common.HexToHash("0x1"), common.HexToHash("0x2")
	for _, tc := range []struct {
		name   string
		log    []common.Hash
		filter [][]common.Hash
		want   bool
	}{
		{"no conditions", nil, nil, true},
		{"existing wildcard", []common.Hash{a}, [][]common.Hash{{}}, true},
		{"missing null position", []common.Hash{a}, [][]common.Hash{{a}, nil}, true},
		{"missing empty position", []common.Hash{a}, [][]common.Hash{{a}, {}}, true},
		{"all wildcard positions", []common.Hash{a}, [][]common.Hash{nil, nil}, true},
		{"wildcard without topics", nil, [][]common.Hash{nil}, true},
		{"missing required topic", []common.Hash{a}, [][]common.Hash{{a}, {b}}, false},
		{"wildcard then missing required topic", []common.Hash{a}, [][]common.Hash{nil, {b}}, false},
		{"wildcard hole", []common.Hash{a, b}, [][]common.Hash{nil, {b}}, true},
		{"matching alternative", []common.Hash{a}, [][]common.Hash{{b, a}}, true},
		{"nonmatching topic", []common.Hash{a}, [][]common.Hash{{b}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MatchEthLogTopics(&web3Types.Log{Topics: tc.log}, tc.filter); got != tc.want {
				t.Fatalf("matched %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNilBoundaries(t *testing.T) {
	if IsInterfaceValNil([1]int{}) {
		t.Fatal("array is not nil")
	}
	if !IsInterfaceValNil((*int)(nil)) {
		t.Fatal("typed nil not recognized")
	}
	if IsLegacyEthTx(nil) || IsLegacyEthTx(&web3Types.TransactionDetail{}) {
		t.Fatal("missing signature is not legacy")
	}
}
