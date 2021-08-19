package rpc

import (
	"github.com/pkg/errors"
)

// Uniform infura rpc errors to comply with fullnode rpc errors.

func invalidParamsError(reason string) error {
	return errors.Errorf("Invalid params: %v.", reason)
}
