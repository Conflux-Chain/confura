package util

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/sirupsen/logrus"
)

func MustMarshalJson(v any) []byte {
	if IsInterfaceValNil(v) {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to marshal data to JSON, value = %+v", v)
	}
	return data
}

func MustUnmarshalJson(data []byte, v any) {
	if err := json.Unmarshal(data, v); err != nil {
		logrus.WithError(err).Fatalf(
			"Failed to unmarshal JSON data, v = %v, data = %x", v, data,
		)
	}
}

func MustMarshalRLP(v any) []byte {
	if IsInterfaceValNil(v) {
		return nil
	}

	data, err := rlp.EncodeToBytes(v)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to marshal data to RLP, value = %+v", v)
	}

	return data
}

func MustUnmarshalRLP(data []byte, v any) {
	if err := rlp.DecodeBytes(data, v); err != nil {
		logrus.WithError(err).Fatalf(
			"Failed to unmarshal RLP data, v = %v, data = %x", v, data,
		)
	}
}
