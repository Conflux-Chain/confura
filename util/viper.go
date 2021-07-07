package util

import (
	"strings"

	"github.com/spf13/viper"
)

type viperGetter func(key string) interface{}
type ViperKeyGetter struct {
	Key    string
	Getter viperGetter
}

// ViperSub is used to fix viper.Sub ignores enviroment variables while unmarshal into struct.
// More info for this fix: https://github.com/spf13/viper/issues/1012#issuecomment-757862260
// Besides, for enviroment variables set to be used for some special types like []string,
// specified getter must be provided to parse the envoriment variables correctly.
func ViperSub(v *viper.Viper, name string, keyGetters ...*ViperKeyGetter) *viper.Viper {
	kgSettings := make(map[string]viperGetter)
	for _, kg := range keyGetters {
		kgSettings[kg.Key] = kg.Getter
	}

	r := viper.New()
	for _, key := range v.AllKeys() {
		if strings.Index(key, name+".") == 0 {
			if getter, ok := kgSettings[key]; ok {
				r.Set(key[len(name)+1:], getter(key))
				continue
			}

			r.Set(key[len(name)+1:], v.Get(key))
		}
	}
	return r
}
