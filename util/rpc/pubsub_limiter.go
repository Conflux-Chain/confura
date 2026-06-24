package rpc

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"sync"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

const unknownPubSubClientIP = "unknown"

type pubSubSessionContextKey struct{}

// PubSubLimitConfig controls resource bounds for WebSocket PubSub traffic.
// A zero value for any field disables that specific bound.
type PubSubLimitConfig struct {
	MaxConnections                int `default:"10000"`
	MaxConnectionsPerIP           int `default:"20"`
	MaxSubscriptions              int `default:"50000"`
	MaxSubscriptionsPerIP         int `default:"100"`
	MaxSubscriptionsPerConnection int `default:"20"`
}

// PubSubLimitError describes which PubSub resource bound was exceeded.
type PubSubLimitError struct {
	Resource string
	Scope    string
	Limit    int
}

func (e *PubSubLimitError) Error() string {
	return fmt.Sprintf("pubsub %s limit exceeded for %s: max %d", e.Resource, e.Scope, e.Limit)
}

// PubSubLimiterStats is a snapshot of active PubSub resource usage.
type PubSubLimiterStats struct {
	Connections       int
	ConnectionsByIP   map[string]int
	Subscriptions     int
	SubscriptionsByIP map[string]int
}

// PubSubLimiter tracks WebSocket PubSub connection and subscription quotas.
type PubSubLimiter struct {
	mu     sync.Mutex
	config PubSubLimitConfig

	nextConnectionID uint64

	connections     int
	connectionsByIP map[string]int

	subscriptions     int
	subscriptionsByIP map[string]int
}

// PubSubSession represents one accepted WebSocket connection.
type PubSubSession struct {
	id      uint64
	ip      string
	limiter *PubSubLimiter

	closed        bool
	subscriptions int
}

// PubSubSubscriptionPermit is the quota permit for one active PubSub subscription.
type PubSubSubscriptionPermit struct {
	session *PubSubSession
	once    sync.Once
}

// NewPubSubLimiterFromViper builds a PubSubLimiter from requestControl.pubsub.
func NewPubSubLimiterFromViper() *PubSubLimiter {
	var cfg PubSubLimitConfig
	viper.MustUnmarshalKey("requestControl.pubsub", &cfg)
	return NewPubSubLimiter(cfg)
}

// NewPubSubLimiter builds a PubSubLimiter with the supplied bounds.
func NewPubSubLimiter(config PubSubLimitConfig) *PubSubLimiter {
	return &PubSubLimiter{
		config:            config,
		connectionsByIP:   make(map[string]int),
		subscriptionsByIP: make(map[string]int),
	}
}

// Stats returns a consistent copy of current PubSub resource counters.
func (l *PubSubLimiter) Stats() PubSubLimiterStats {
	l.mu.Lock()
	defer l.mu.Unlock()

	return PubSubLimiterStats{
		Connections:       l.connections,
		ConnectionsByIP:   maps.Clone(l.connectionsByIP),
		Subscriptions:     l.subscriptions,
		SubscriptionsByIP: maps.Clone(l.subscriptionsByIP),
	}
}

// AcquireConnection reserves quota for one WebSocket connection.
func (l *PubSubLimiter) AcquireConnection(ip string) (*PubSubSession, error) {
	ip = normalizePubSubClientIP(ip)

	l.mu.Lock()
	defer l.mu.Unlock()

	if limitExceeded(l.config.MaxConnections, l.connections) {
		return nil, &PubSubLimitError{Resource: "connection", Scope: "all clients", Limit: l.config.MaxConnections}
	}
	if limitExceeded(l.config.MaxConnectionsPerIP, l.connectionsByIP[ip]) {
		return nil, &PubSubLimitError{Resource: "connection", Scope: "ip " + ip, Limit: l.config.MaxConnectionsPerIP}
	}

	l.nextConnectionID++
	session := &PubSubSession{id: l.nextConnectionID, ip: ip, limiter: l}

	l.connections++
	l.connectionsByIP[ip]++

	return session, nil
}

// AcquireSubscription reserves quota for one subscription on this WebSocket connection.
func (s *PubSubSession) AcquireSubscription() (*PubSubSubscriptionPermit, error) {
	if s == nil || s.limiter == nil {
		return nil, nil
	}

	return s.limiter.acquireSubscription(s)
}

// Close releases this WebSocket connection and any remaining subscription quota.
func (s *PubSubSession) Close() {
	if s == nil || s.limiter == nil {
		return
	}

	s.limiter.releaseConnection(s)
}

// Release releases this subscription quota once.
func (permit *PubSubSubscriptionPermit) Release() {
	if permit == nil || permit.session == nil || permit.session.limiter == nil {
		return
	}

	permit.once.Do(func() {
		permit.session.limiter.releaseSubscription(permit.session)
	})
}

func (l *PubSubLimiter) acquireSubscription(session *PubSubSession) (*PubSubSubscriptionPermit, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if session.closed {
		return nil, &PubSubLimitError{Resource: "subscription", Scope: "closed connection", Limit: 0}
	}
	if limitExceeded(l.config.MaxSubscriptions, l.subscriptions) {
		return nil, &PubSubLimitError{Resource: "subscription", Scope: "all clients", Limit: l.config.MaxSubscriptions}
	}
	if limitExceeded(l.config.MaxSubscriptionsPerIP, l.subscriptionsByIP[session.ip]) {
		return nil, &PubSubLimitError{Resource: "subscription", Scope: "ip " + session.ip, Limit: l.config.MaxSubscriptionsPerIP}
	}
	if limitExceeded(l.config.MaxSubscriptionsPerConnection, session.subscriptions) {
		return nil, &PubSubLimitError{
			Resource: "subscription",
			Scope:    fmt.Sprintf("connection %d", session.id),
			Limit:    l.config.MaxSubscriptionsPerConnection,
		}
	}

	l.subscriptions++
	l.subscriptionsByIP[session.ip]++
	session.subscriptions++

	return &PubSubSubscriptionPermit{session: session}, nil
}

func (l *PubSubLimiter) releaseSubscription(session *PubSubSession) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if session.closed || session.subscriptions == 0 {
		return
	}

	session.subscriptions--
	l.subscriptions--
	decrementCount(l.subscriptionsByIP, session.ip, 1)
}

func (l *PubSubLimiter) releaseConnection(session *PubSubSession) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if session.closed {
		return
	}

	session.closed = true
	l.connections--
	decrementCount(l.connectionsByIP, session.ip, 1)

	// Connection teardown can race with subscription goroutine cleanup. Releasing
	// remaining subscription counters here prevents leaked quota if the handler exits
	// before every subscription loop observes rpcSub.Err or notifier.Closed.
	if session.subscriptions > 0 {
		l.subscriptions -= session.subscriptions
		decrementCount(l.subscriptionsByIP, session.ip, session.subscriptions)
		session.subscriptions = 0
	}
}

func pubSubConnectionLimitMiddleware(limiter *PubSubLimiter) handlers.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := pubSubClientIPFromContext(r.Context())
			session, err := limiter.AcquireConnection(ip)
			if err != nil {
				logrus.WithError(err).
					WithField("clientIP", ip).Debug("PubSub connection limit exceeded")
				http.Error(w, err.Error(), http.StatusTooManyRequests)
				return
			}
			defer session.Close()

			ctx := ContextWithPubSubSession(r.Context(), session)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func pubSubClientIPFromContext(ctx context.Context) string {
	// httpMiddleware is the single place that interprets proxy headers; the
	// PubSub limiter consumes the normalized value from context.
	clientIP, _ := handlers.GetIPAddressFromContext(ctx)
	return clientIP
}

// ContextWithPubSubSession stores the current PubSub session in a request context.
func ContextWithPubSubSession(ctx context.Context, session *PubSubSession) context.Context {
	return context.WithValue(ctx, pubSubSessionContextKey{}, session)
}

// PubSubSessionFromContext returns the current PubSub session from a request context.
func PubSubSessionFromContext(ctx context.Context) (*PubSubSession, bool) {
	session, ok := ctx.Value(pubSubSessionContextKey{}).(*PubSubSession)
	return session, ok && session != nil
}

func limitExceeded(limit, current int) bool {
	return limit > 0 && current >= limit
}

func normalizePubSubClientIP(ip string) string {
	if len(ip) == 0 {
		return unknownPubSubClientIP
	}
	return ip
}

func decrementCount(counts map[string]int, key string, delta int) {
	next := counts[key] - delta
	if next <= 0 {
		delete(counts, key)
		return
	}

	counts[key] = next
}
