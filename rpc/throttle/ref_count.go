package throttle

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

var DefaultExpiration = time.Minute

// RefCounter allows to throttle based on reference counter.
type RefCounter struct {
	client *redis.Client
	quota  int64
	ctx    context.Context
}

func NewRefCounter(client *redis.Client, quota int64) *RefCounter {
	if quota <= 0 {
		logrus.WithField("quota", quota).Fatal("Invalid quota to create RefCounter instance")
	}

	return &RefCounter{
		client: client,
		quota:  quota,
		ctx:    context.Background(),
	}
}

func (rc *RefCounter) Ref(key string, expiration ...time.Duration) bool {
	if rc.client == nil {
		return false
	}

	timeout := DefaultExpiration
	if len(expiration) > 0 && expiration[0] > 0 {
		timeout = expiration[0]
	}

	batchOps := rc.client.TxPipeline()
	incrCmd := batchOps.Incr(rc.ctx, key)
	batchOps.Expire(rc.ctx, key, timeout)
	if _, err := batchOps.Exec(rc.ctx); err != nil {
		// treat as no quota if any error occurred
		logrus.WithError(err).WithField("key", key).Warn("Failed to exec batch cmd to add ref")
		return false
	}

	count, err := incrCmd.Result()
	if err != nil {
		// treat as no quota if any error occurred
		logrus.WithError(err).WithField("key", key).Warn("Failed to incr to add ref")
		return false
	}

	if count <= rc.quota {
		return true
	}

	// auto unref if no quota available
	rc.UnrefAsync(key)

	return false
}

func (rc *RefCounter) SafeUnref(key string) {
	if rc.client == nil {
		return
	}

	for {
		_, err := rc.client.Decr(rc.ctx, key).Result()
		if err == nil {
			break
		}

		logrus.WithError(err).WithField("key", key).Warn("Failed to decr to unref, try again")
		time.Sleep(100 * time.Millisecond)
	}
}

func (rc *RefCounter) UnrefAsync(key string) {
	go func() {
		rc.SafeUnref(key)
	}()
}
