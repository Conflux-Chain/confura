// Package viper preserves Confura's defaults-before-configuration decoding.
package viper

import (
	"strings"

	upstream "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// UnmarshalKey retains explicit zero values, which can disable configured limits.
// Delegate key discovery and resolution to upstream, but apply defaults before
// decoding so false, zero and empty strings can override nonzero defaults.
func UnmarshalKey(key string, target interface{}, resolvers ...upstream.ValueResolver) error {
	resolved := viper.New()
	prefix := strings.ToLower(key)
	if prefix != "" {
		prefix += "."
	}
	err := upstream.UnmarshalKey(key, &struct{}{}, func(fullKey string) (interface{}, bool) {
		value := viper.Get(fullKey)
		if len(resolvers) > 0 {
			if override, ok := resolvers[0](fullKey); ok {
				value = override
			}
		}
		if value != nil {
			resolved.Set(strings.TrimPrefix(fullKey, prefix), value)
		}
		return nil, true // Collect values without decoding them into the empty struct.
	})
	if err != nil {
		return err
	}
	defaults.SetDefaults(target)
	return resolved.Unmarshal(target, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		mapstructure.TextUnmarshallerHookFunc(),
	)), func(config *mapstructure.DecoderConfig) {
		config.ZeroFields = true // Explicit slices replace defaults instead of retaining their tail.
	})
}

func MustUnmarshalKey(key string, target interface{}, resolvers ...upstream.ValueResolver) {
	if err := UnmarshalKey(key, target, resolvers...); err != nil {
		logrus.WithError(err).WithField("key", key).Fatal("Failed to unmarshal data from viper")
	}
}
