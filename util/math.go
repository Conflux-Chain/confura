package util

import (
	"math/rand"
	"time"
)

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}

	return b
}

func RandUint64(limit uint64) uint64 {
	if limit == 0 {
		return 0
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return (r.Uint64() % limit)
}
