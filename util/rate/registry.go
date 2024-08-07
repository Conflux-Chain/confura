package rate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/rate"
	"github.com/Conflux-Chain/go-conflux-util/rate/http"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

	mu      sync.Mutex
	kloader *KeyLoader

	// all available strategies
	strategies    map[string]*Strategy // strategy name => *Strategy
	id2Strategies map[uint32]*Strategy // strategy id => *Strategy
}

func NewRegistry(kloader *KeyLoader, valFactory acl.ValidatorFactory) *Registry {
	m := &Registry{
		kloader:       kloader,
		aclRegistry:   newAclRegistry(kloader, valFactory),
		strategies:    make(map[string]*Strategy),
		id2Strategies: make(map[uint32]*Strategy),
	}

	m.Registry = http.NewRegistry(m)
	go m.ScheduleGC(GCScheduleInterval)

	return m
}

// implements `http.LimiterFactory`

func (r *Registry) GetGroupAndKey(
	ctx context.Context,
	resource string,
) (group, key string, err error) {
	authId, ok := handlers.GetAuthIdFromContext(ctx)
	if !ok {
		// use default strategy if not authenticated
		return r.genDefaultGroupAndKey(ctx, resource)
	}

	if vip, ok := handlers.VipStatusFromContext(ctx); ok {
		// use vip strategy with corresponding tier
		return r.genVipGroupAndKey(ctx, resource, authId, vip)
	}

	if ki, ok := r.kloader.Load(authId); ok && ki != nil {
		// use strategy with corresponding key info
		return r.genKeyInfoGroupAndKey(ctx, resource, authId, ki)
	}

	// use default strategy as fallback
	return r.genDefaultGroupAndKey(ctx, resource)
}

func (r *Registry) Create(ctx context.Context, resource, group string) (rate.Limiter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stg, ok := r.strategies[group]
	if !ok {
		return nil, errors.New("strategy not found")
	}

	opt, ok := stg.LimitOptions[resource]
	if !ok {
		return nil, errors.New("limit rule not found")
	}

	return r.createWithOption(opt)
}

func (r *Registry) genDefaultGroupAndKey(
	ctx context.Context,
	resource string,
) (group, key string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stg, ok := r.strategies[DefaultStrategy]
	if !ok { // no default strategy
		logrus.WithField("resource", resource).Debug("Default rate limit strategy not configured")
		return
	}

	if _, ok := stg.LimitOptions[resource]; !ok {
		// limit rule not defined
		return
	}

	ip, _ := handlers.GetIPAddressFromContext(ctx)
	key = fmt.Sprintf("ip:%v", ip)

	return stg.Name, key, nil
}

func (r *Registry) genVipGroupAndKey(
	ctx context.Context,
	resource, limitKey string,
	vip *handlers.VipStatus,
) (group, key string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stg, ok := r.getVipStrategy(vip.Tier)
	if !ok { // vip strategy not configured
		logrus.WithFields(logrus.Fields{
			"limitKey": limitKey,
			"resource": resource,
			"vip":      vip,
		}).Info("VIP strategy not found")
		return
	}

	if _, ok := stg.LimitOptions[resource]; !ok {
		// limit rule not defined
		return
	}

	key = fmt.Sprintf("key:%v", limitKey)
	return stg.Name, key, nil
}

func (r *Registry) genKeyInfoGroupAndKey(
	ctx context.Context,
	resource, limitKey string,
	ki *KeyInfo,
) (group, key string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stg, ok := r.id2Strategies[ki.SID]
	if !ok {
		logrus.WithFields(logrus.Fields{
			"limitKey": limitKey,
			"resource": resource,
			"keyInfo":  ki,
		}).Warn("Rate limit strategy not found")
		return
	}

	if _, ok := stg.LimitOptions[resource]; !ok {
		// limit rule not defined
		return
	}

	group = stg.Name

	switch ki.Type {
	case LimitTypeByIp: // limit by key-based IP
		ip, _ := handlers.GetIPAddressFromContext(ctx)
		key = fmt.Sprintf("key:%v/ip:%v", limitKey, ip)

	case LimitTypeByKey: // limit by key only
		key = fmt.Sprintf("key:%v", limitKey)

	default:
		err = errors.New("invalid limit type")
	}

	return group, key, err
}

func (r *Registry) createWithOption(option interface{}) (l rate.Limiter, err error) {
	switch opt := option.(type) {
	case FixedWindowOption:
		l = rate.NewFixedWindow(opt.Interval, opt.Quota)
	case TokenBucketOption:
		l = rate.NewTokenBucket(int(opt.Rate), opt.Burst)
	default:
		err = errors.New("invalid limit option")
	}

	return l, err
}
