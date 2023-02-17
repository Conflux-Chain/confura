package rate

import (
	"sync"
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
	SVip int       // svip level
}

type KeysetFilter struct {
	SIDs   []uint32 // strategy IDs
	KeySet []string // limit key set
	Limit  int      // result limit size (<= 0 means none)
}

// ksLoadFunc loads limit keyset with specific filter from wherever eg., store
type ksLoadFunc func(filter *KeysetFilter) ([]*KeyInfo, error)

type KeyLoader struct {
	mu sync.Mutex
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
	cv, expired, found := l.keyCache.GetNoExp(key)
	if found && !expired { // found in cache
		return cv.(*KeyInfo), true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	cv, expired, found = l.keyCache.GetNoExp(key)
	if found && !expired { // double check
		return cv.(*KeyInfo), true
	}

	if found && expired {
		// extend lifespan for expired cache kv temporarliy for performance
		l.keyCache.Add(key, cv.(*KeyInfo))
	}

	// load key info from db
	ki, err := l.rawLoad(key)
	if err != nil {
		logrus.WithField("key", key).WithError(err).Error("Key loader failed to load limit key info")
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
