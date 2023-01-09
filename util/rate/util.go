package rate

import (
	"math/rand"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
)

const (
	LimitKeyLength = 32
)

func GenerateRandomLimitKey(limitType LimitType) (string, error) {
	data := make([]byte, LimitKeyLength)

	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	r.Read(data)

	if limitType != LimitTypeByKey && limitType != LimitTypeByIp {
		return "", errors.New("invalid limit type")
	}

	secretb := []byte(base58.Encode(data))
	secretb[0] = '0' + uint8(limitType)

	return string(secretb), nil
}
