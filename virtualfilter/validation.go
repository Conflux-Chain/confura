package virtualfilter

import (
	"encoding/json"
	"errors"
	"fmt"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	ethtypes "github.com/openweb3/web3go/types"
)

var errInvalidFilterChanges = errors.New("invalid upstream filter result")

// Validate before changing either persistent or in-memory session state.
func validateLogChanges(changes filterChanges) error {
	switch c := changes.(type) {
	case *ethtypes.FilterChanges:
		if c == nil || len(c.Hashes) != 0 {
			return errInvalidFilterChanges
		}
	case *cfxtypes.CfxFilterChanges:
		if c == nil || c.Type == "hash" || len(c.Hashes) != 0 {
			return errInvalidFilterChanges
		}
		for _, item := range c.Logs {
			if item == nil || (item.Log == nil) == (item.ChainReorg == nil) {
				return errInvalidFilterChanges
			}
			if item.ChainReorg != nil {
				if item.RevertTo == nil || !item.RevertTo.ToInt().IsUint64() {
					return errInvalidFilterChanges
				}
			} else if item.BlockHash == nil || item.EpochNumber == nil || item.LogIndex == nil ||
				!item.EpochNumber.ToInt().IsUint64() || !item.LogIndex.ToInt().IsUint64() {
				return errInvalidFilterChanges
			}
		}
	default:
		return errInvalidFilterChanges
	}
	return nil
}

// Decode outside the provider so malformed results cannot be retried as
// transport failures by a configured SDK retry middleware.
type upstreamLogResult = json.RawMessage

func decodeUpstreamLogResult(raw upstreamLogResult, target interface{}) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("%w: %v", errInvalidFilterChanges, err)
	}
	return nil
}
