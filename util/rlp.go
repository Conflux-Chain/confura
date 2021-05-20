package util

import (
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/sirupsen/logrus"
)

func MustMarshalRLP(v interface{}) []byte {
	if v == nil {
		return nil
	}

	data, err := rlp.EncodeToBytes(v)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to marshal data to RLP, value = %+v", v)
	}

	return data
}

func MustUnmarshalRLP(data []byte, v interface{}) {
	if err := rlp.DecodeBytes(data, v); err != nil {
		logrus.WithError(err).Fatalf("Failed to unmarshal RLP data, v = %v, data = %x", v, data)
	}
}
