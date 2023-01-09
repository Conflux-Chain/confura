package v2

import (
	"time"

	"github.com/Conflux-Chain/confura/util"
	"github.com/sirupsen/logrus"
)

const (
	LimitKeyCacheSize     = 5000
	LimitKeyExpirationTTL = 90 * time.Second
)

type KeyInfo struct {
	SID  uint32    // bound strategy ID
	Key  string    // limit key
	Type LimitType // limit type
}

type KeysetFilter struct {
	SIDs   []uint32 // strategy IDs
	KeySet []string // limit key set
	Limit  int      // result limit size (<= 0 means none)
}

// ksLoadFunc loads limit keyset with specific filter from wherever eg., store
type ksLoadFunc func(filter *KeysetFilter) ([]*KeyInfo, error)

type KeyLoader struct {
	// raw keyset load function
	ksload ksLoadFunc
	// limit key cache: limit key => *KeyInfo (nil if missing)
	keyCache *util.ExpirableLruCache
}

func NewKeyLoader(ksload ksLoadFunc) *KeyLoader {
	kl := &KeyLoader{
		ksload: ksload,
		keyCache: util.NewExpirableLruCache(
			LimitKeyCacheSize, LimitKeyExpirationTTL,
		),
	}

	// warm up limit key cache for better performance
	kl.warmUpKeyCache()

	return kl
}

// Load loads key info from cache or raw loading from somewhere else
// if cache missed.
func (l *KeyLoader) Load(key string) (*KeyInfo, bool) {
	// load from cache first
	if cv, ok := l.keyCache.Get(key); ok {
		// found in cache
		return cv.(*KeyInfo), true
	}

	// load from store if not found in cache
	ki, err := l.rawLoad(key)
	if err != nil {
		logrus.WithError(err).Error("Failed to load limit key info")
		return nil, false
	}

	// cache limit key
	l.keyCache.Add(key, ki)
	return ki, true
}

func (kl *KeyLoader) rawLoad(key string) (*KeyInfo, error) {
	kinfos, err := kl.ksload(&KeysetFilter{KeySet: []string{key}})
	if err == nil && len(kinfos) > 0 {
		return kinfos[0], nil
	}

	return nil, err
}

func (kl *KeyLoader) warmUpKeyCache() {
	kis, err := kl.ksload(&KeysetFilter{Limit: (LimitKeyCacheSize * 3 / 4)})
	if err != nil {
		logrus.WithError(err).Warn("Failed to load limit keyset to warm up cache")
		return
	}

	for i := range kis {
		kl.keyCache.Add(kis[i].Key, kis[i])
	}

	logrus.WithField("totalKeys", len(kis)).Info("Limit keyset loaded to cache")
}
