package util

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
)

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
		logrus.WithError(err).Fatalf("Failed to unmarshal JSON data, v = %v, data = %x", v, data)
	}
}
