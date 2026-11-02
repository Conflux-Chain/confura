// Package viper preserves Confura's configuration contract independently of
// changes to dependency initialization: require a config file, never load .env,
// and apply default tags before decoding explicit values (including 0/false).
package viper

import (
	"os"
	"sort"
	"strings"

	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	base "github.com/spf13/viper"
)

var envKeyPrefix string

type ValueResolver func(string) (interface{}, bool)

func MustInit(prefix string, paths ...string) {
	if err := Init(prefix, paths...); err != nil {
		logrus.WithError(err).Fatal("Failed to read configuration")
	}
}

// Init is startup-only; Viper's global state must not be changed concurrently.
func Init(prefix string, paths ...string) error {
	envKeyPrefix = strings.ToUpper(prefix + "_")
	base.AutomaticEnv()
	base.SetEnvPrefix(prefix)
	base.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	// Bind environment-only settings so dependency decoders can discover them
	// through AllKeys without running their dotenv-loading initializer.
	for key, env := range environmentKeys() {
		if err := base.BindEnv(key, env); err != nil {
			return err
		}
	}
	if len(paths) > 0 {
		base.SetConfigFile(paths[0])
	} else {
		base.SetConfigName("config")
		base.AddConfigPath(".")
		base.AddConfigPath("./config")
	}
	return base.ReadInConfig()
}

func environmentKeys() map[string]string {
	keys := make(map[string]string)
	if envKeyPrefix == "" {
		return keys
	}
	for _, entry := range os.Environ() {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || !strings.HasPrefix(name, envKeyPrefix) {
			continue
		}
		key := strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(name, envKeyPrefix), "_", "."))
		keys[key] = name
	}
	return keys
}

func MustUnmarshalKey(key string, out interface{}, resolvers ...ValueResolver) {
	if err := UnmarshalKey(key, out, resolvers...); err != nil {
		logrus.WithError(err).WithField("key", key).Fatal("Failed to decode configuration")
	}
}

func UnmarshalKey(key string, out interface{}, resolvers ...ValueResolver) error {
	defaults.SetDefaults(out)
	prefix := strings.ToLower(key)
	if prefix != "" {
		prefix += "."
	}
	keys := make(map[string]bool)
	for _, k := range base.AllKeys() {
		if strings.HasPrefix(k, prefix) {
			keys[k] = true
		}
	}
	for k := range environmentKeys() {
		if strings.HasPrefix(k, prefix) {
			keys[k] = true
		}
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	sub := base.New()
	for _, k := range ordered {
		value := base.Get(k)
		if len(resolvers) > 0 {
			if v, ok := resolvers[0](k); ok {
				value = v
			}
		}
		if value != nil {
			sub.Set(strings.TrimPrefix(k, prefix), value)
		}
	}
	hook := mapstructure.ComposeDecodeHookFunc(mapstructure.StringToTimeDurationHookFunc(), mapstructure.StringToSliceHookFunc(","), mapstructure.TextUnmarshallerHookFunc())
	return sub.Unmarshal(out, base.DecodeHook(hook))
}
