package election

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"gorm.io/gorm"
)

type leadershipKey struct{}
type leadershipTerm struct {
	manager *DlockLeaderManager
	nonce   string
	ready   bool
}

// WithLeadership binds work to its original term, including work that outlives a reelection.
func WithLeadership(ctx context.Context, lm LeaderManager) context.Context {
	if l, ok := lm.(*DlockLeaderManager); ok {
		return context.WithValue(ctx, leadershipKey{}, leadershipTerm{l, l.term(), l.electedCallbacksDone.Load()})
	}
	return ctx
}

func (l *DlockLeaderManager) Guard(ctx context.Context) func(*gorm.DB) error {
	nonce := l.termFromContext(ctx)
	return func(tx *gorm.DB) error {
		if err := l.validateTerm(ctx, nonce); err != nil {
			return err
		}
		// Await may have returned in an earlier term. Work captured before the
		// current term's callbacks completed must not gain permission afterward.
		if term, ok := ctx.Value(leadershipKey{}).(leadershipTerm); ok && !term.ready {
			return store.ErrLeaderRenewal
		}
		err := l.lockMan.RenewInTx(ctx, tx, l.lockIntent(nonce))
		if errors.Is(err, dlock.ErrLockNotHeld) {
			return store.ErrLeaderRenewal
		}
		return err
	}
}

// termFromContext preserves the original term of work across reelections.
func (l *DlockLeaderManager) termFromContext(ctx context.Context) string {
	nonce := l.term()
	if term, ok := ctx.Value(leadershipKey{}).(leadershipTerm); ok {
		if term.manager != l {
			nonce = ""
		} else {
			nonce = term.nonce
		}
	}
	return nonce
}

func (l *DlockLeaderManager) validateTerm(ctx context.Context, nonce string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nonce == "" || nonce != l.term() || l.electionStatus.Load() != StatusElected {
		return store.ErrLeaderRenewal
	}
	return nil
}
