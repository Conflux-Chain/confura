package node

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type RedisRepartitionResolver struct {
	client *redis.Client
	ttl    time.Duration
	ctx    context.Context
	logger *logrus.Entry
}

func NewRedisRepartitionResolver(client *redis.Client, ttl time.Duration) *RedisRepartitionResolver {
	return &RedisRepartitionResolver{
		client: client,
		ttl:    ttl,
		ctx:    context.Background(),
		logger: logrus.WithField("module", "RedisRepartitionResolver"),
	}
}

func redisRepartitionKey(key uint64) string {
	return fmt.Sprintf("node:repartition:key:%v", key)
}

func (r *RedisRepartitionResolver) Get(key uint64) (string, bool) {
	redisKey := redisRepartitionKey(key)
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
	redisKey := redisRepartitionKey(key)
	if err := r.client.Set(r.ctx, redisKey, value, r.ttl).Err(); err != nil {
		r.logger.WithError(err).WithField("key", redisKey).Error("Failed to set key-value to redis")
	}
}
