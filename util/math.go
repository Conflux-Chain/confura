package util

import (
	"math/rand"
	"time"
)

func RandUint64(limit uint64) uint64 {
	if limit == 0 {
		return 0
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return (r.Uint64() % limit)
}
