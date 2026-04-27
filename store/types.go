package store

import (
	"github.com/pkg/errors"
)

var (
	// custom errors
	ErrNotFound               = errors.New("not found")
	ErrUnsupported            = errors.New("not supported")
	ErrEpochPivotSwitched     = errors.New("epoch pivot switched")
	ErrContinousEpochRequired = errors.New("continous epoch required")
	ErrAlreadyPruned          = errors.New("data already pruned")
	ErrChainReorged           = errors.New("chain re-orged")
	ErrLeaderRenewal          = errors.New("leadership renewal failure")
)
