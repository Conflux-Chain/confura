package rate

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
)

type aclRegistry struct {
	mu      sync.Mutex
	kloader *KeyLoader

	// all available allowlists
	allowlists map[uint32]*acl.AllowList // allowlist id => *acl.AllowList
	validators map[uint32]*acl.Validator // allowlist id => *acl.Validator
}

func newAclRegistry(kloader *KeyLoader) *aclRegistry {
	return &aclRegistry{
		kloader:    kloader,
		allowlists: make(map[uint32]*acl.AllowList),
		validators: make(map[uint32]*acl.Validator),
	}
}

func (r *aclRegistry) Allow(ctx acl.Context) error {
	if v, ok := r.assignValidator(ctx); ok {
		return v.Validate(ctx)
	}

	return nil
}

func (r *aclRegistry) assignValidator(ctx context.Context) (*acl.Validator, bool) {
	authId, ok := handlers.GetAuthIdFromContext(ctx)
	if !ok { // use default allowlist if not authenticated
		return r.getValidator(0)
	}

	if _, ok := handlers.VipStatusFromContext(ctx); ok {
		// use default allowlsit for web3pay user
		return r.getValidator(0)
	}

	if ki, ok := r.kloader.Load(authId); ok && ki != nil {
		// use allowlist with corresponding key info
		return r.getValidator(ki.AclID)
	}

	// use default allowlist as fallback
	return r.getValidator(0)
}

func (r *aclRegistry) getValidator(aclID uint32) (*acl.Validator, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	v, ok := r.validators[aclID]
	return v, ok
}
