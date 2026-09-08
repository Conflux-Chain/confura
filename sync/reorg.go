package sync

func findCommonAncestor(
	latest uint64,
	parentHash string,
	hashAt func(uint64) (string, bool, error),
) (uint64, bool, error) {
	if parentHash == "" {
		return 0, false, nil
	}

	for current := latest; ; current-- {
		hash, ok, err := hashAt(current)
		if err != nil {
			return 0, false, err
		}
		if ok && hash == parentHash {
			return current, true, nil
		}
		if current == 0 {
			break
		}
	}

	return 0, false, nil
}
