package election

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/viper"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/Conflux-Chain/go-conflux-util/health"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
	// Await waits for leadership and completion of elected callbacks, or cancellation.
	Await(ctx context.Context) bool
	// Extend extends the leadership lease
	Extend(ctx context.Context) error
	// Guard captures the current term and authorizes a write transaction until it ends.
	Guard(ctx context.Context) func(*gorm.DB) error
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

// MustNewLeaderManagerFromViper creates a leader manager sharing the data store database.
func MustNewLeaderManagerFromViper(db *gorm.DB, elecKey string) LeaderManager {
	var conf Config
	viper.MustUnmarshalKey("sync.election", &conf)

	if conf.Enabled {
		logrus.WithField("config", conf).Info("HA leader election enabled")
		return NewDlockLeaderManager(db, conf, elecKey)
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

	electionKey          string       // election key
	electionStatus       atomic.Int32 // election status
	electedCallbacksDone atomic.Bool  // readiness for new work after elected callbacks

	lockMan        *mysqlLockManager
	nonce          atomic.Value // a distinct nonce for each leadership term
	lockExpiryTime atomic.Int64 // expiry timestamp for the dlock

	mu               sync.Mutex        // mutex lock
	cancel           atomic.Value      // function to cancel the campaign process.
	electedCallbacks []ElectedCallback // leader elected callback functions
	oustedCallbacks  []OustedCallback  // leader ousted callback functions
	errorCallbacks   []ErrorCallback   // leader election error callback functions
}

// NewDlockLeaderManager creates a new `LeaderManager` with the provided configuration and election key.
func NewDlockLeaderManager(db *gorm.DB, conf Config, elecKey string) *DlockLeaderManager {
	conf.ID += "#" + uuid.NewString()[:8] // append a unique suffix to the `Id`
	return &DlockLeaderManager{
		Config:      conf,
		lockMan:     newMysqlLockManager(db),
		electionKey: elecKey,
	}
}

func (l *DlockLeaderManager) Identity() string {
	return l.ID
}

// Await admits work only after elected callbacks finish and while the lease is valid.
func (l *DlockLeaderManager) Await(ctx context.Context) bool {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			if l.electionStatus.Load() == StatusElected &&
				l.electedCallbacksDone.Load() &&
				l.lockExpiryTime.Load() > time.Now().Unix() {
				return true
			}

			timer.Reset(l.Retry)
		}
	}
}

// Extend extends leadership lease.
func (l *DlockLeaderManager) Extend(ctx context.Context) error {
	if l.electionStatus.Load() != StatusElected {
		return store.ErrLeaderRenewal
	}
	return l.renew(ctx)
}

// Campaign starts the election process, which will run in a goroutine until context canceled.
func (l *DlockLeaderManager) Campaign(ctx context.Context) {
	newCtx, cancelFn := context.WithCancel(ctx)
	l.cancel.Store(cancelFn)

	healthStatus := health.NewCounter()
	logger := logrus.WithFields(logrus.Fields{"electionKey": l.electionKey, "leaderID": l.ID})

	etlog := func(err error) {
		recovered, unhealthy, unrecovered, failures := healthStatus.OnError(err)
		switch {
		case recovered:
			logger.Warn("Leader election recovered from failures")
		case unhealthy:
			logger.WithError(err).Error("Leader election becomes unhealthy")
		case unrecovered:
			logger.WithFields(logrus.Fields{
				"failures": failures,
			}).WithError(err).Warn("Leader election still not recovered after failures")
		}
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
	if v := l.cancel.Load(); v != nil {
		if cancelFn, ok := v.(context.CancelFunc); ok {
			cancelFn()
		}
	}

	err := l.lockMan.Release(context.Background(), l.lockIntent(l.term()))
	if errors.Is(err, dlock.ErrLockNotHeld) {
		return nil
	}
	return err
}

// retryLock consistently retries to acquire a lock until success.
func (l *DlockLeaderManager) retryLock(ctx context.Context) error {
	l.electedCallbacksDone.Store(false)
	l.nonce.Store(uuid.NewString())
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
			err := l.Extend(ctx)
			if err == nil {
				renewTimer.Reset(l.Renew)
				continue
			}

			if errors.Is(err, store.ErrLeaderRenewal) {
				return nil
			}

			return err
		}
	}
}

// acquireLock helper function to acquire a distributed lock.
func (l *DlockLeaderManager) acquireLock(ctx context.Context) error {
	start := time.Now()
	if l.term() == "" {
		l.nonce.Store(uuid.NewString())
	}
	err := l.lockMan.Acquire(ctx, l.lockIntent(l.term()))
	if err == nil {
		l.lockExpiryTime.Store(start.Add(l.Lease).Unix())
		return nil
	}

	return err
}

func (l *DlockLeaderManager) term() string {
	if v := l.nonce.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// renew cannot reacquire an expired lease, even if its nonce has not changed.
func (l *DlockLeaderManager) renew(ctx context.Context) error {
	start := time.Now()
	nonce := l.termFromContext(ctx)
	if err := l.validateTerm(ctx, nonce); err != nil {
		return err
	}
	err := l.lockMan.Renew(ctx, l.lockIntent(nonce))
	if errors.Is(err, dlock.ErrLockNotHeld) {
		return store.ErrLeaderRenewal
	}
	if err != nil {
		return err
	}
	l.lockExpiryTime.Store(start.Add(l.Lease).Unix())
	return nil
}

func (l *DlockLeaderManager) lockIntent(nonce string) *dlock.LockIntent {
	return dlock.NewLockIntent(l.electionKey, nonce, l.Lease)
}

// OnElected registers a callback function to be invoked on leader elected.
func (l *DlockLeaderManager) OnElected(cb ElectedCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.electedCallbacks = append(l.electedCallbacks, cb)
}

// onElected is called after the current process is elected as the leader.
func (l *DlockLeaderManager) onElected(ctx context.Context) {
	l.electedCallbacksDone.Store(false)
	logrus.WithFields(logrus.Fields{
		"electionKey": l.electionKey,
		"leaderID":    l.ID,
	}).Info("Leader elected")

	l.mu.Lock()
	defer l.mu.Unlock()

	l.electionStatus.Store(StatusElected)
	for _, cb := range l.electedCallbacks {
		cb(ctx, l)
	}
	// Callbacks observe elected status, but new work waits for their state resets.
	if ctx.Err() == nil {
		l.electedCallbacksDone.Store(true)
	}
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
	l.electedCallbacksDone.Store(false)
	logrus.WithFields(logrus.Fields{
		"electionKey": l.electionKey,
		"leaderID":    l.ID,
	}).Info("Leader ousted")

	l.electionStatus.Store(StatusOusted)

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cb := range l.oustedCallbacks {
		cb(ctx, l)
	}
}

// OnError registers a callback function to be invoked on election error.
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
type noopLeaderManager struct {
	mu               sync.Mutex        // mutex lock
	cancel           atomic.Value      // function to cancel the campaign process.
	oustedCallbacks  []OustedCallback  // leader ousted callback functions
	electedCallbacks []ElectedCallback // leader elected callback functions
}

func NewNoopLeaderManager() *noopLeaderManager {
	return &noopLeaderManager{}
}

func (l *noopLeaderManager) Identity() string                 { return "noop" }
func (l *noopLeaderManager) Extend(ctx context.Context) error { return nil }
func (l *noopLeaderManager) Guard(ctx context.Context) func(*gorm.DB) error {
	return func(*gorm.DB) error { return ctx.Err() }
}
func (l *noopLeaderManager) OnError(cb ErrorCallback) { /* do nothing */ }

func (l *noopLeaderManager) Await(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

func (l *noopLeaderManager) Stop() error {
	if v := l.cancel.Load(); v != nil {
		if cancelFn, ok := v.(context.CancelFunc); ok {
			cancelFn()
		}
	}
	return nil
}

func (l *noopLeaderManager) Campaign(ctx context.Context) {
	ctx, cancelFn := context.WithCancel(ctx)
	l.cancel.Store(cancelFn)

	l.onElected(ctx)

	<-ctx.Done()

	l.onOusted(ctx)
}

func (l *noopLeaderManager) OnOusted(cb OustedCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.oustedCallbacks = append(l.oustedCallbacks, cb)
}

func (l *noopLeaderManager) onOusted(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cb := range l.oustedCallbacks {
		cb(ctx, l)
	}
}

func (l *noopLeaderManager) OnElected(cb ElectedCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.electedCallbacks = append(l.electedCallbacks, cb)
}

func (l *noopLeaderManager) onElected(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cb := range l.electedCallbacks {
		cb(ctx, l)
	}
}
