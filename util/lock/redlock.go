package lock

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	rsredis "github.com/go-redsync/redsync/v4/redis"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type RedlockManager struct {
	*redsync.Redsync
}

func MustNewRedlockManagerFromViper() (*RedlockManager, bool) {
	var conf struct {
		Enabled   bool
		RedisUrls []string
	}
	viper.MustUnmarshalKey("redlock", &conf)

	if !conf.Enabled {
		return nil, false
	}

	rlman, err := NewRedlockManager(conf.RedisUrls)
	if err != nil {
		logrus.WithField("urls", conf.RedisUrls).
			WithError(err).Fatal("Failed to new redlock manager from viper")
	}
	return rlman, true
}

func NewRedlockManager(redisUrls []string) (*RedlockManager, error) {
	var pools []rsredis.Pool
	for _, url := range redisUrls {
		opt, err := redis.ParseURL(url)
		if err != nil {
			return nil, errors.WithMessagef(err, "invalid Redis URL: %v", url)
		}

		client := redis.NewClient(opt)

		// Test connection
		if _, err := client.Ping(context.Background()).Result(); err != nil {
			return nil, errors.WithMessagef(err, "can not ping Redis: %v", url)
		}

		pools = append(pools, goredis.NewPool(client))
	}

	// Create an instance of redisync to be used to obtain a distrubuted
	// mutual exclusion lock.
	return &RedlockManager{Redsync: redsync.New(pools...)}, nil
}
