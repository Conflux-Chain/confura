package store

type LogV2 struct {
	ID          uint64
	ContractID  uint64
	BlockNumber uint64
	Epoch       uint64
	Topic0      string
	Topic1      string
	Topic2      string
	Topic3      string
	LogIndex    uint64
	Extra       []byte
}

func (log *LogV2) cmp(other *LogV2) int {
	if log.BlockNumber < other.BlockNumber {
		return -1
	}

	if log.BlockNumber > other.BlockNumber {
		return 1
	}

	if log.LogIndex < other.LogIndex {
		return -1
	}

	if log.LogIndex > other.LogIndex {
		return 1
	}

	return 0
}

type LogSlice []*LogV2

func (s LogSlice) Len() int           { return len(s) }
func (s LogSlice) Less(i, j int) bool { return s[i].cmp(s[j]) < 0 }
func (s LogSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
