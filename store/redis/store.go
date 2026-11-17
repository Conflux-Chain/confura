package redis

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

func MustNewRedisClient(url string) *redis.Client {
	client, err := NewRedisClient(context.Background(), url)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize redis")
	}
	return client
}

func NewRedisClient(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opt)

	// Test connection
	if _, err := client.Ping(ctx).Result(); err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}
