package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/pkg/errors"
)

var (
	ErrRateLimitRegistryNotConfigured = errors.New("rate limit registry is not configured")
)

type diagnosticAPI struct {
	registry *rate.Registry
}

func (api *diagnosticAPI) ListRateLimitStrategies(ctx context.Context) ([]*rate.Strategy, error) {
	if api.registry == nil {
		return nil, ErrRateLimitRegistryNotConfigured
	}
	return api.registry.Strategies(), nil
}

type RateLimitStatus struct {
	Strategy  *rate.Strategy    `json:"strategy"`
	LimitType string            `json:"limitType"`
	Info      rate.UserMetaData `json:"info"`
}

func (api *diagnosticAPI) GetRateLimitStatus(ctx context.Context) (status RateLimitStatus, err error) {
	if api.registry == nil {
		return status, ErrRateLimitRegistryNotConfigured
	}

	decision, err := api.registry.Resolve(ctx)
	if err != nil {
		return status, errors.WithMessage(err, "failed to resolve rate limit strategy")
	}

	if decision != nil {
		status = RateLimitStatus{
			Strategy:  decision.Strategy,
			LimitType: decision.LimitType.String(),
			Info:      decision.UserMeta,
		}
	}
	return status, nil
}
