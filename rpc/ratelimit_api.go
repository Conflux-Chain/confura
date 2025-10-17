package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/pkg/errors"
)

var (
	ErrRateLimitRegistryNotConfigured = errors.New("ratelimit registry is not configured")
)

type rateLimitAPI struct {
	registry *rate.Registry
}

func (api *rateLimitAPI) ListStrategies(ctx context.Context) ([]*rate.Strategy, error) {
	if api.registry == nil {
		return nil, ErrRateLimitRegistryNotConfigured
	}
	return api.registry.Strategies(), nil
}

type UserRateLimitInfo struct {
	Strategy  *rate.Strategy    `json:"strategy"`
	LimitType string            `json:"limitType"`
	Details   rate.UserMetaData `json:"details"`
}

func (api *rateLimitAPI) GetMyInfo(ctx context.Context) (info UserRateLimitInfo, err error) {
	if api.registry == nil {
		return info, ErrRateLimitRegistryNotConfigured
	}

	decision, err := api.registry.Resolve(ctx)
	if err != nil {
		return info, errors.WithMessage(err, "failed to resolve rate limit strategy")
	}

	if decision != nil {
		info = UserRateLimitInfo{
			Strategy:  decision.Strategy,
			LimitType: decision.LimitType.String(),
			Details:   decision.UserMeta,
		}
	}
	return info, nil
}
