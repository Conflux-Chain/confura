package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/sirupsen/logrus"
)

// DecodeJSONObject rejects null and trailing input. Strict mode also rejects
// unknown fields in ordinary structs; custom unmarshaling must validate itself.
func DecodeJSONObject(data []byte, dst interface{}, strict bool) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("expected a JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if strict {
		d.DisallowUnknownFields()
	}
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(interface{})); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON input")
	}
	return nil
}

func MustMarshalJson(v interface{}) []byte {
	if IsInterfaceValNil(v) {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to marshal data to JSON, value = %+v", v)
	}
	return data
}

func MustUnmarshalJson(data []byte, v interface{}) {
	if err := json.Unmarshal(data, v); err != nil {
		logrus.WithError(err).Fatalf(
			"Failed to unmarshal JSON data, v = %v, data = %x", v, data,
		)
	}
}

func MustMarshalRLP(v interface{}) []byte {
	if IsInterfaceValNil(v) {
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
		logrus.WithError(err).Fatalf(
			"Failed to unmarshal RLP data, v = %v, data = %x", v, data,
		)
	}
}
