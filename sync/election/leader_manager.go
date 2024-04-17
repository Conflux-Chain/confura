package election

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type LeaderStatus = int32

const (
	StatusInit LeaderStatus = iota
	StatusElected
	StatusOusted
)

var (
	_ LeaderManager = (*noopLeaderManager)(nil)
	_ LeaderManager = (*DlockLeaderManager)(nil)
)

// ElectedCallback is a type alias for the callback function executed upon leader elected.
type ElectedCallback func(ctx context.Context, lm LeaderManager)

// OustedCallback is a type alias for the callback function executed upon leader ousted.
type OustedCallback func(ctx context.Context, lm LeaderManager)

// ErrorCallback is a type alias for the callback function executed upon election error.
type ErrorCallback func(ctx context.Context, lm LeaderManager, err error)

type LeaderManager interface {
	// Identity returns leader identity
	Identity() string
	// Wait until being elected as leader or context canceled
	Await(ctx context.Context) bool
	// Extend extends the leadership lease
	Extend(ctx context.Context) error
	// Campaign starts the leader election process
	Campaign(ctx context.Context)
	// Stop stops the leader election process
	Stop() error
	// OnElected registers a leader elected callback function.
	OnElected(cb ElectedCallback)
	// OnOusted registers a leader ousted callback function.
	OnOusted(cb OustedCallback)
	// OnError registers an election error callback function.
	OnError(cb ErrorCallback)
}

// MustNewLeaderManagerFromViper creates a new LeaderManager with the given LockManager and election key.
func MustNewLeaderManagerFromViper(dlm *dlock.LockManager, elecKey string) LeaderManager {
	var conf Config
	viper.MustUnmarshalKey("sync.election", &conf)

	if conf.Enabled {
		logrus.WithField("config", conf).Info("HA leader election enabled")
		return NewDlockLeaderManager(dlm, conf, elecKey)
	}

	return &noopLeaderManager{}
}

// Config holds the configuration for the leader election.
type Config struct {
	// Leader election enabled or not?
	Enabled bool
	// Unique identifier for the leader
	ID string `default:"leader"`
	// Duration of the leader term
	Lease time.Duration `default:"1m"`
	// The time interval between retries of becoming the leader
	Retry time.Duration `default:"5s"`
	// The time interval trying to renew the leader term
	Renew time.Duration `default:"15s"`
}

// DlockLeaderManager manages the leader election process using distributed lock.
type DlockLeaderManager struct {
	Config // election configuration

	electionKey    string       // election key
	electionStatus LeaderStatus // election status

	lockMan        *dlock.LockManager // provides lock operations
	lockExpiryTime int64              // expiry timestamp for the dlock

	mu               sync.Mutex         // mutex lock
	cancel           context.CancelFunc // function to cancel the campaign process.
	electedCallbacks []ElectedCallback  // leader elected callback functions
	oustedCallbacks  []OustedCallback   // leader ousted callback functions
	errorCallbacks   []ErrorCallback    // leader election error callback functions
}

// NewDlockLeaderManager creates a new `LeaderManager` with the provided configuration and election key.
func NewDlockLeaderManager(dlm *dlock.LockManager, conf Config, elecKey string) *DlockLeaderManager {
	conf.ID += "#" + uuid.NewString()[:8] // append a unique suffix to the `Id`
	return &DlockLeaderManager{
		Config:      conf,
		lockMan:     dlm,
		electionKey: elecKey,
	}
}

func (l *DlockLeaderManager) Identity() string {
	return l.ID
}

// Await blocks until being elected as leader or context canceled.
func (l *DlockLeaderManager) Await(ctx context.Context) bool {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			if atomic.LoadInt32(&l.electionStatus) == StatusElected &&
				atomic.LoadInt64(&l.lockExpiryTime) > time.Now().Unix() {
				return true
			}

			timer.Reset(l.Retry)
		}
	}
}

// Extend extends leadership lease.
func (l *DlockLeaderManager) Extend(ctx context.Context) error {
	if atomic.LoadInt32(&l.electionStatus) == StatusElected {
		if err := l.acquireLock(ctx); err != nil {
			return errors.WithMessage(store.ErrLeaderRenewal, err.Error())
		}

		return nil
	}

	return store.ErrLeaderRenewal
}

// Campaign starts the election process, which will run in a goroutine until contex canceled.
func (l *DlockLeaderManager) Campaign(ctx context.Context) {
	newCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	logger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	etlog := func(err error) {
		logger.Log(
			logrus.WithFields(logrus.Fields{
				"electionKey": l.electionKey,
				"leaderID":    l.ID,
			}), err, "Leader election campaign error",
		)
	}

	for {
		// retry to acquire lock at first
		err := l.retryLock(newCtx)
		etlog(err)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			l.onError(newCtx, err)
			time.Sleep(l.Retry)
			continue
		}

		// if we got here we are the leader
		l.onElected(newCtx)

		// renew the lock lease
		err = l.renewLock(newCtx)
		etlog(err)

		// if we got here we are not the leader anymore
		l.onOusted(newCtx)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			l.onError(newCtx, err)
		}
	}
}

// Stop stops the leader election process and resigns from the leadership if appliable.
func (l *DlockLeaderManager) Stop() error {
	if l.cancel != nil {
		l.cancel()
	}

	return l.lockMan.Release(context.Background(), l.lockIntent())
}

// retryLock consistently retries to acquire a lock until success.
func (l *DlockLeaderManager) retryLock(ctx context.Context) error {
	retryTimer := time.NewTimer(0)
	defer retryTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retryTimer.C:
			err := l.acquireLock(ctx)
			if errors.Is(err, dlock.ErrLockAcquisitionFailed) {
				retryTimer.Reset(l.Retry)
				continue
			}

			return err
		}
	}
}

// renewLock consistently renews a lock lease until any error.
func (l *DlockLeaderManager) renewLock(ctx context.Context) error {
	renewTimer := time.NewTimer(l.Renew)
	defer renewTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-renewTimer.C:
			err := l.acquireLock(ctx)
			if err == nil {
				renewTimer.Reset(l.Renew)
				continue
			}

			if errors.Is(err, dlock.ErrLockAcquisitionFailed) {
				return nil
			}

			return err
		}
	}
}

// acquireLock helper function to acquire a distrubted lock.
func (l *DlockLeaderManager) acquireLock(ctx context.Context) error {
	start := time.Now()
	err := l.lockMan.Acquire(ctx, l.lockIntent())
	if err == nil {
		atomic.StoreInt64(&l.lockExpiryTime, start.Add(l.Lease).Unix())
		return nil
	}

	return err
}

// lockIntent helper function to assemble a lock intent.
func (l *DlockLeaderManager) lockIntent() *dlock.LockIntent {
	return &dlock.LockIntent{
		Key:   l.electionKey,
		Nonce: l.ID,
		Lease: l.Lease,
	}
}

// OnElected registers a callback function to be invoked on leader elected.
func (l *DlockLeaderManager) OnElected(cb ElectedCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.electedCallbacks = append(l.electedCallbacks, cb)
}

// onElected is called after the current process is elected as the leader.
func (l *DlockLeaderManager) onElected(ctx context.Context) {
	logrus.WithFields(logrus.Fields{
		"electionKey": l.electionKey,
		"leaderID":    l.ID,
	}).Info("Leader elected")

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cb := range l.electedCallbacks {
		cb(ctx, l)
	}

	atomic.StoreInt32(&l.electionStatus, StatusElected)
}

// OnOusted registers a callback function to be invoked on leader ousted.
func (l *DlockLeaderManager) OnOusted(cb OustedCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.oustedCallbacks = append(l.oustedCallbacks, cb)
}

// onOusted is called after the current process is not the leader anymore,
// either because of an error or failure of renewing lease.
func (l *DlockLeaderManager) onOusted(ctx context.Context) {
	logrus.WithFields(logrus.Fields{
		"electionKey": l.electionKey,
		"leaderID":    l.ID,
	}).Info("Leader ousted")

	atomic.StoreInt32(&l.electionStatus, int32(StatusOusted))

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cb := range l.electedCallbacks {
		cb(ctx, l)
	}
}

// OnOusted registers a callback function to be invoked on leader ousted.
func (l *DlockLeaderManager) OnError(cb ErrorCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.errorCallbacks = append(l.errorCallbacks, cb)
}

// OnError is called when an error occurs during the election process
// ethier on retry or renew stage.
func (l *DlockLeaderManager) onError(ctx context.Context, err error) {
	logrus.WithFields(logrus.Fields{
		"electionKey": l.electionKey,
		"leaderID":    l.ID,
	}).WithError(err).Info("Leader election error")

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cb := range l.errorCallbacks {
		cb(ctx, l, err)
	}
}

// noopLeaderManager dummy leader manager that does nothing at all.
type noopLeaderManager struct{}

func (l *noopLeaderManager) Identity() string                 { return "noop" }
func (l *noopLeaderManager) Extend(ctx context.Context) error { return nil }
func (l *noopLeaderManager) Await(ctx context.Context) bool   { return true }
func (l *noopLeaderManager) Campaign(ctx context.Context)     { /* do nothing */ }
func (l *noopLeaderManager) Stop() error                      { return nil }
func (l *noopLeaderManager) OnElected(cb ElectedCallback)     { /* do nothing */ }
func (l *noopLeaderManager) OnOusted(cb OustedCallback)       { /* do nothing */ }
func (l *noopLeaderManager) OnError(cb ErrorCallback)         { /* do nothing */ }
