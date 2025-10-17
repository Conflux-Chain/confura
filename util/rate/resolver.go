package rate

import (
	"context"
	"fmt"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type StrategyResolverType string

const ( // Resolver types
	ResolverDefault     StrategyResolverType = "default"
	ResolverWeb3pay     StrategyResolverType = "web3pay"
	ResolverProvisioned StrategyResolverType = "provisioned"
)

const ( // User types
	UserTypeGuest       = "guest"
	UserTypeWeb3Pay     = "web3pay_user"
	UserTypeProvisioned = "provisioned_user"
)

var (
	_ StrategyResolver = (*DefaultResolver)(nil)
	_ StrategyResolver = (*Web3payResolver)(nil)
	_ StrategyResolver = (*ProvisionedResolver)(nil)
	_ StrategyResolver = (*CompositeResolver)(nil)
)

type UserMetaData map[string]any

// StrategyDecision is the result of resolving which rate limit strategy
// applies to the current request, including its runtime parameters.
type StrategyDecision struct {
	Strategy  *Strategy            // matched static strategy
	LimitType LimitType            // rate limit type
	LimitKey  string               // unique key for limiter
	UserMeta  UserMetaData         // runtime user metadata
	MatchedBy StrategyResolverType // which resolver matched
}

type StrategyResolver interface {
	Resolve(ctx context.Context, reg *Registry) (decision *StrategyDecision, err error)
}

type DefaultResolver struct{}

func (r *DefaultResolver) Resolve(ctx context.Context, reg *Registry) (*StrategyDecision, error) {
	ip, _ := handlers.GetIPAddressFromContext(ctx)
	stg := reg.strategies[DefaultStrategy]
	if stg == nil {
		logrus.Debug("Default rate limit strategy not configured")
	}

	decision := &StrategyDecision{
		Strategy:  stg,
		LimitType: LimitTypeByIp,
		LimitKey:  fmt.Sprintf("ip:%v", ip),
		MatchedBy: ResolverDefault,
		UserMeta: UserMetaData{
			"userType": UserTypeGuest,
			"clientIp": ip,
		},
	}

	return decision, nil
}

type Web3payResolver struct{}

func (r *Web3payResolver) Resolve(ctx context.Context, reg *Registry) (*StrategyDecision, error) {
	authId, ok := handlers.GetAuthIdFromContext(ctx)
	if !ok { // not authenticated?
		return nil, nil
	}

	vip, ok := handlers.VipStatusFromContext(ctx)
	if !ok { // not a web3pay user?
		return nil, nil
	}

	stg, ok := reg.getVipStrategy(vip.Tier)
	if stg == nil {
		logrus.WithField("vip", vip).Debug("Web3pay VIP strategy not configured")
	}

	decision := &StrategyDecision{
		Strategy:  stg,
		LimitKey:  fmt.Sprintf("key:%v", authId),
		LimitType: LimitTypeByKey,
		MatchedBy: ResolverWeb3pay,
		UserMeta: UserMetaData{
			"userType":    UserTypeWeb3Pay,
			"web3payInfo": vip,
		},
	}
	return decision, nil
}

type ProvisionedResolver struct{}

func (r *ProvisionedResolver) Resolve(ctx context.Context, reg *Registry) (*StrategyDecision, error) {
	authId, ok := handlers.GetAuthIdFromContext(ctx)
	if !ok { // not authenticated?
		return nil, nil
	}

	ki, ok := reg.kloader.Load(authId)
	if !ok || ki == nil { // no key info found?
		return nil, nil
	}

	stg, _ := reg.id2Strategies[ki.SID]
	if stg == nil {
		logrus.WithField("keyInfo", ki).Debug("Custom rate limit strategy not configured")
	}

	var limitKey string
	ip, _ := handlers.GetIPAddressFromContext(ctx)

	switch ki.Type {
	case LimitTypeByIp: // limit by key-based IP
		limitKey = fmt.Sprintf("key:%v/ip:%v", authId, ip)

	case LimitTypeByKey: // limit by key only
		limitKey = fmt.Sprintf("key:%v", authId)

	default:
		return nil, errors.Errorf("invalid limit type %d", ki.Type)
	}

	decision := &StrategyDecision{
		Strategy:  stg,
		LimitKey:  limitKey,
		LimitType: ki.Type,
		MatchedBy: ResolverProvisioned,
		UserMeta: UserMetaData{
			"userType": UserTypeProvisioned,
			"clientIp": ip,
			"apiKey":   ki.Key,
		},
	}
	return decision, nil
}

// CompositeResolver tries multiple resolvers in order until one matches.
type CompositeResolver struct {
	resolvers []StrategyResolver
}

func NewCompositeResolver(resolvers ...StrategyResolver) *CompositeResolver {
	return &CompositeResolver{resolvers: resolvers}
}

func (r *CompositeResolver) Resolve(ctx context.Context, reg *Registry) (*StrategyDecision, error) {
	for _, resolver := range r.resolvers {
		decision, err := resolver.Resolve(ctx, reg)
		if err != nil {
			return nil, err
		}
		if decision != nil {
			return decision, nil
		}
	}
	return nil, nil
}
