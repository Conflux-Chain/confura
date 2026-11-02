package rate

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Conflux-Chain/confura/util"
	"github.com/sirupsen/logrus"
)

func TestKeyLoaderCoalescesConcurrentMisses(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	l := &KeyLoader{keyCache: util.NewExpirableLruCache(LimitKeyCacheSize, LimitKeyExpirationTTL)}
	l.ksload = func(*KeysetFilter) ([]*KeyInfo, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return []*KeyInfo{{Key: "fixture", SID: 2}}, nil
	}
	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ki, ok := l.Load("fixture")
			if !ok || ki.SID != 2 {
				t.Error("incorrect lookup")
			}
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("got %d DB reads", calls.Load())
	}
}

func TestKeyLoaderDoesNotLogCredentials(t *testing.T) {
	const key = "secret-fixture-not-a-real-key"
	var out bytes.Buffer
	old := logrus.StandardLogger().Out
	logrus.SetOutput(&out)
	defer logrus.SetOutput(old)
	l := &KeyLoader{keyCache: util.NewExpirableLruCache(1, LimitKeyExpirationTTL), ksload: func(*KeysetFilter) ([]*KeyInfo, error) { return nil, errors.New("query key=" + key) }}
	if _, ok := l.Load(key); ok {
		t.Fatal("error treated as successful load")
	}
	if strings.Contains(out.String(), key) {
		t.Fatalf("credential logged: %s", out.String())
	}
	if ki, ok := l.Load(key); !ok || ki != nil {
		t.Fatal("missing negative cache")
	}
}
