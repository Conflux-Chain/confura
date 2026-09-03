package rpc

import "github.com/Conflux-Chain/confura/rpc/handler"

// validateScanLogsRequest rejects request-shape errors that can be determined
// before normalization performs any full-node lookups.
func validateScanLogsRequest(
	withPivotAssumption bool,
	cursor *handler.ScanLogCursor,
	assumptionProvided bool,
) error {
	if withPivotAssumption && cursor != nil && !assumptionProvided {
		return handler.NewScanLogsErrorf(
			handler.ErrScanLogsInvalidParams, "missing pivot assumption",
		)
	}
	return nil
}
