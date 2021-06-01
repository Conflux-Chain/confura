package util

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

func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}

	return b
}
