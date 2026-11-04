package rate

import (
	"crypto/rand"

	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
)

const (
	LimitKeyLength = 32
)

func GenerateRandomLimitKey(limitType LimitType) (string, error) {
	if limitType != LimitTypeByKey && limitType != LimitTypeByIp {
		return "", errors.New("invalid limit type")
	}
	data := make([]byte, LimitKeyLength)
	if _, err := rand.Read(data); err != nil {
		return "", errors.Wrap(err, "failed to generate limit key")
	}

	secretb := []byte(base58.Encode(data))
	secretb[0] = '0' + uint8(limitType)

	return string(secretb), nil
}
