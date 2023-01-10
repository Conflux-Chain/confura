package rate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/rate"
	"github.com/Conflux-Chain/go-conflux-util/rate/http"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	GCScheduleInterval = 5 * time.Minute
)

type Registry struct {
	*http.Registry

	mu      sync.Mutex
	kloader *KeyLoader

	// all available strategies
	strategies    map[string]*Strategy // strategy name => *Strategy
	id2Strategies map[uint32]*Strategy // strategy id => *Strategy
}

func NewRegistry(kloader *KeyLoader) *Registry {
	m := &Registry{
		kloader:       kloader,
		strategies:    make(map[string]*Strategy),
		id2Strategies: make(map[uint32]*Strategy),
	}

	m.Registry = http.NewRegistry(m)
	go m.ScheduleGC(GCScheduleInterval)

	return m
}

// SVipStatusFromContext returns SVIP status from context
func (r *Registry) SVipStatusFromContext(ctx context.Context) (svip int, ok bool) {
	limitKey, ok := handlers.GetAccessTokenFromContext(ctx)
	if !ok || len(limitKey) == 0 { // no limit key provided
		return
	}

	if ki, ok := r.kloader.Load(limitKey); ok && ki != nil {
		svip, ok = ki.SVip, true
	}

	return svip, ok
}

// implements `http.LimiterFactory`

func (r *Registry) GetGroupAndKey(
	ctx context.Context,
	resource string,
) (group, key string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	limitKey, ok := handlers.GetAccessTokenFromContext(ctx)
	if !ok || len(limitKey) == 0 {
		// use default strategy if no limit key provided
		return r.genDefaultGroupAndKey(ctx, resource)
	}

	vip, ok := handlers.VipStatusFromContext(ctx)
	if ok && vip != nil && vip.Tier != handlers.VipTierNone {
		// use vip strategy with corresponding tier
		return r.genVipGroupAndKey(ctx, resource, limitKey, vip)
	}

	if ki, ok := r.kloader.Load(limitKey); ok && ki != nil {
		// use strategy with corresponding key info
		return r.genKeyInfoGroupAndKey(ctx, resource, limitKey, ki)
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
	stg, ok := r.strategies[DefaultStrategy]
	if !ok { // no default strategy
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
	stg, ok := r.getVipStrategy(vip.Tier)
	if !ok { // vip strategy not configured
		logrus.WithField("vip", vip).Info("No VIP strategy configured")
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
	stg, ok := r.id2Strategies[ki.SID]
	if !ok {
		err = errors.New("strategy not found")
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
