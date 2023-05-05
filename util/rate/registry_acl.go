package rate

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/sirupsen/logrus"
)

type aclRegistry struct {
	mu sync.Mutex

	kloader *KeyLoader

	defaultValidator acl.Validator
	valFactory       acl.ValidatorFactory

	// all available allowlists
	allowlists map[uint32]*acl.AllowList // allowlist id => *acl.AllowList
	validators map[uint32]acl.Validator  // allowlist id => *acl.Validator
}

func newAclRegistry(kloader *KeyLoader, valFactory acl.ValidatorFactory) *aclRegistry {
	return &aclRegistry{
		kloader:    kloader,
		valFactory: valFactory,
		allowlists: make(map[uint32]*acl.AllowList),
		validators: make(map[uint32]acl.Validator),
	}
}

func (r *aclRegistry) Allow(ctx acl.Context) error {
	if v, ok := r.assignValidator(ctx); ok {
		return v.Validate(ctx)
	}

	return nil
}

func (r *aclRegistry) assignValidator(ctx context.Context) (acl.Validator, bool) {
	authId, ok := handlers.GetAuthIdFromContext(ctx)
	if !ok { // use default allowlist if not authenticated
		return r.getDefaultValidator()
	}

	if vs, ok := handlers.VipStatusFromContext(ctx); ok {
		// use VIP allowlsit with corresponding tier
		return r.getVipValidator(vs)
	}

	if ki, ok := r.kloader.Load(authId); ok && ki != nil {
		// use allowlist with corresponding key info
		return r.getKeyInfoValidator(ki)
	}

	// use default allowlist as fallback
	return r.getDefaultValidator()
}

func (r *aclRegistry) getVipValidator(vip *handlers.VipStatus) (acl.Validator, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// assemble vip allowlist name by tier
	vipAllowList := fmt.Sprintf("vip%d", vip.Tier)

	for _, al := range r.allowlists {
		if strings.EqualFold(al.Name, vipAllowList) {
			v, ok := r.validators[al.ID]
			return v, ok
		}
	}

	return nil, false
}

func (r *aclRegistry) getDefaultValidator() (acl.Validator, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if v := r.defaultValidator; v != nil {
		return v, true
	}

	return nil, false
}

func (r *aclRegistry) getKeyInfoValidator(ki *KeyInfo) (acl.Validator, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	v, ok := r.validators[ki.AclID]
	return v, ok
}

// allowlists reloading

func (r *aclRegistry) reloadAclAllowLists(rc *Config, lastCs *ConfigCheckSums) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// remove allowlists
	for alid, al := range r.allowlists {
		if _, ok := rc.AllowLists[alid]; !ok {
			r.removeAllowList(al)
			logrus.WithField("allowList", al).Info("Allow list removed")
		}
	}

	// add or update allowlists
	for alid, al := range rc.AllowLists {
		oldal, ok := r.allowlists[alid]
		if !ok { // add
			r.addAllowList(al)
			logrus.WithField("allowList", al).Info("Allow list added")
			continue
		}

		if lastCs.AllowLists[alid] != rc.CheckSums.AllowLists[alid] { // update
			r.updateAllowList(oldal, al)
			logrus.WithField("allowList", al).Info("Allow list updated")
		}
	}
}

func (r *aclRegistry) addAllowList(al *acl.AllowList) {
	r.allowlists[al.ID] = al
	r.validators[al.ID] = r.valFactory(al)

	if strings.EqualFold(al.Name, acl.DefaultAllowList) {
		r.defaultValidator = r.validators[al.ID]
	}
}

func (r *aclRegistry) removeAllowList(al *acl.AllowList) {
	delete(r.allowlists, al.ID)
	delete(r.validators, al.ID)

	if strings.EqualFold(al.Name, acl.DefaultAllowList) {
		r.defaultValidator = nil
	}
}

func (r *aclRegistry) updateAllowList(old, new *acl.AllowList) {
	r.removeAllowList(old)
	r.addAllowList(new)
}
