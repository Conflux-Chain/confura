package rate

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/rate"
	"github.com/Conflux-Chain/go-conflux-util/rate/http"
	"github.com/pkg/errors"
)

const (
	GCScheduleInterval = 5 * time.Minute
)

type SVipStatus = KeyInfo

// SVipStatusFromContext returns SVIP status from context
func SVipStatusFromContext(ctx context.Context) (*SVipStatus, bool) {
	limitKey, ok := handlers.GetAccessTokenFromContext(ctx)
	if !ok || len(limitKey) == 0 { // no limit key provided
		return nil, false
	}

	reg, ok := ctx.Value(handlers.CtxKeyRateRegistry).(*Registry)
	if !ok || reg == nil {
		return nil, false
	}

	ki, ok := reg.kloader.Load(limitKey)
	return ki, ok && ki != nil
}

type Registry struct {
	*http.Registry
	*aclRegistry

	mu                sync.Mutex
	limiterStrategies atomic.Value // immutable map[string]*Strategy; Create never takes mu
	kloader           *KeyLoader

	// all available strategies
	strategies    map[string]*Strategy // strategy name => *Strategy
	id2Strategies map[uint32]*Strategy // strategy id => *Strategy

	// strategy resolver
	resolver *CompositeResolver
}

func NewRegistry(kloader *KeyLoader, valFactory acl.ValidatorFactory) *Registry {
	m := &Registry{
		kloader:       kloader,
		aclRegistry:   newAclRegistry(kloader, valFactory),
		strategies:    make(map[string]*Strategy),
		id2Strategies: make(map[uint32]*Strategy),
		resolver: NewCompositeResolver(
			&Web3payResolver{},
			&ProvisionedResolver{},
			&DefaultResolver{},
		),
	}

	m.publishLimiterStrategies()
	m.Registry = http.NewRegistry(m)
	go m.ScheduleGC(GCScheduleInterval)

	return m
}

func (r *Registry) Strategies() []*Strategy {
	r.mu.Lock()
	defer r.mu.Unlock()

	stgs := make([]*Strategy, 0, len(r.strategies))
	for _, stg := range r.strategies {
		stgs = append(stgs, stg)
	}
	return stgs
}

func (r *Registry) Resolve(ctx context.Context) (*StrategyDecision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.resolver.Resolve(ctx, r)
}

// implements `http.LimiterFactory`

func (r *Registry) GetGroupAndKey(
	ctx context.Context,
	resource string,
) (group, key string, err error) {
	decision, err := r.Resolve(ctx)
	if err != nil {
		return group, key, errors.WithMessage(err, "failed to resolve rate limit strategy")
	}

	// No rate limit applied if no strategy matched
	if decision == nil || decision.Strategy == nil {
		return group, key, nil
	}

	if _, ok := decision.Strategy.LimitOptions[resource]; !ok {
		// No rate limit applied if rule not defined
		return group, key, nil
	}

	group, key = decision.Strategy.Name, decision.LimitKey
	return group, key, nil
}

func (r *Registry) Create(ctx context.Context, resource, group string) (rate.Limiter, error) {
	strategies := r.limiterStrategies.Load().(map[string]*Strategy)
	stg, ok := strategies[group]
	if !ok {
		return nil, errors.New("strategy not found")
	}

	opt, ok := stg.LimitOptions[resource]
	if !ok {
		return nil, errors.New("limit rule not found")
	}

	return r.createWithOption(opt)
}

func (r *Registry) createWithOption(option interface{}) (l rate.Limiter, err error) {
	switch opt := option.(type) {
	case FixedWindowOption:
		if opt.Interval <= 0 || opt.Quota <= 0 {
			return nil, errors.New("fixed window interval and quota must be positive")
		}
		l = rate.NewFixedWindow(opt.Interval, opt.Quota)
	case TokenBucketOption:
		if opt.Rate <= 0 || math.IsNaN(float64(opt.Rate)) || math.IsInf(float64(opt.Rate), 0) {
			return nil, errors.New("token bucket rate must be positive and finite")
		}
		if opt.Burst <= 0 {
			return nil, errors.New("token bucket burst must be positive")
		}
		if float64(opt.Burst)/float64(opt.Rate) >= float64(math.MaxInt64/int64(time.Second)) {
			return nil, errors.New("token bucket refill interval is too large")
		}
		l = rate.NewTokenBucketRate(opt.Rate, opt.Burst)
	default:
		err = errors.New("invalid limit option")
	}

	return l, err
}

// publishLimiterStrategies is called with mu held (or before publication at construction).
// The dependency invokes Create with its own lock held. Publish an immutable
// snapshot so Create does not reverse reload's mu -> registry lock order.
func (r *Registry) publishLimiterStrategies() {
	snapshot := make(map[string]*Strategy, len(r.strategies))
	for name, strategy := range r.strategies {
		snapshot[name] = strategy
	}
	r.limiterStrategies.Store(snapshot)
}
