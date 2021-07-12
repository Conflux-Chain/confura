package node

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type RedisRepartitionResolver struct {
	client    *redis.Client
	ttl       time.Duration
	ctx       context.Context
	keyPrefix string
	logger    *logrus.Entry
}

func NewRedisRepartitionResolver(client *redis.Client, ttl time.Duration, keyPrefix string) *RedisRepartitionResolver {
	return &RedisRepartitionResolver{
		client:    client,
		ttl:       ttl,
		ctx:       context.Background(),
		keyPrefix: keyPrefix,
		logger:    logrus.WithField("module", "RedisRepartitionResolver"),
	}
}

func (r *RedisRepartitionResolver) Get(key uint64) (string, bool) {
	redisKey := redisRepartitionKey(key, r.keyPrefix)
	node, err := r.client.GetEx(r.ctx, redisKey, r.ttl).Result()
	if err == redis.Nil {
		return "", false
	}

	if err != nil {
		r.logger.WithError(err).WithField("key", redisKey).Error("Failed to read key from redis")
		return "", false
	}

	return node, true
}

func (r *RedisRepartitionResolver) Put(key uint64, value string) {
	redisKey := redisRepartitionKey(key, r.keyPrefix)
	if err := r.client.Set(r.ctx, redisKey, value, r.ttl).Err(); err != nil {
		r.logger.WithError(err).WithField("key", redisKey).Error("Failed to set key-value to redis")
	}
}

func redisRepartitionKey(key uint64, prefixs ...string) string {
	prefixs = append(prefixs, "key")
	prefixStr := strings.Join(prefixs, ":")

	return fmt.Sprintf("node:repartition:%v:%v", prefixStr, key)
}
