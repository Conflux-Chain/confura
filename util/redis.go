package util

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

func MustNewRedisClient(url string) *redis.Client {
	opt, err := redis.ParseURL(url)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to parse redis url")
	}

	client := redis.NewClient(opt)

	// Test connection
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		logrus.WithError(err).Fatal("Failed to ping redis")
	}

	return client
}
