package util

import (
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"testing"
)

func TestTopicArity(t *testing.T) {
	h := common.HexToHash("0x1")
	log := &web3Types.Log{Topics: []common.Hash{h}}
	for _, topics := range [][][]common.Hash{{{h}, {}}, {{h}, {h}}, {{}, {}}} {
		if MatchEthLogTopics(log, topics) {
			t.Fatal("accepted missing topic")
		}
	}
	if !MatchEthLogTopics(log, [][]common.Hash{{}}) {
		t.Fatal("rejected existing wildcard position")
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
