package util

import (
	"strings"

	"github.com/spf13/viper"
)

// ViperSub is used to fix viper.Sub ignores enviroment variables while unmarshal into struct.
// More info for this fix: https://github.com/spf13/viper/issues/1012#issuecomment-757862260
func ViperSub(v *viper.Viper, name string) *viper.Viper {
	r := viper.New()
	for _, key := range v.AllKeys() {
		if strings.Index(key, name+".") == 0 {
			r.Set(key[len(name)+1:], v.Get(key))
		}
	}
	return r
}
