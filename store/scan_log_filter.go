package store

// ScanLogFilter contains the predicates supported by keyset log scans.
type ScanLogFilter struct {
	BlockFrom uint64
	BlockTo   uint64
	Contract  string
	Topic0    string
}

// ScanCursor is an exclusive keyset cursor ordered by block number and log index.
type ScanCursor struct {
	BlockNumber uint64
	LogIndex    uint64
}

// ScanLogParams contains the filter and page controls for a keyset log scan.
type ScanLogParams struct {
	Filter  ScanLogFilter
	Cursor  *ScanCursor
	Reverse bool
	Limit   int
}
