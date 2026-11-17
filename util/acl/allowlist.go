package acl

import (
	"fmt"
	"github.com/Conflux-Chain/confura/util"
)

const (
	// pre-defined default allowlist name
	DefaultAllowList = "default"
)

// AllowList access control rule list.
type AllowList struct {
	ID   uint32 // Allowlist ID
	Name string // Allowlist name

	// Any requests which query addresses that are not in the allowlist are rejected.
	ContractAddresses []string

	// The allowed methods list. If the list is empty, all methods will be accepted.
	AllowMethods []string

	// The disallowed methods list. If the list is empty, no methods will be rejected.
	DisallowMethods []string

	// Restricted `User-Agent` request headers
	UserAgents []string

	// Restricted `Origin` request headers
	Origins []string
}

func NewAllowList(id uint32, name string) *AllowList {
	return &AllowList{
		ID:   id,
		Name: name,
	}
}

// DecodeRules never accepts identity from rule JSON. Legacy metadata is ignored
// when reading stored configurations; new input must contain only rule fields.
func (al *AllowList) DecodeRules(data []byte, strict bool) error {
	var rules struct {
		ContractAddresses, AllowMethods, DisallowMethods, UserAgents, Origins []string
	}
	if err := util.DecodeJSONObject(data, &rules, strict); err != nil {
		return err
	}
	al.ContractAddresses, al.AllowMethods, al.DisallowMethods = rules.ContractAddresses, rules.AllowMethods, rules.DisallowMethods
	al.UserAgents, al.Origins = rules.UserAgents, rules.Origins
	return al.ValidateRules()
}

func (al *AllowList) ValidateRules() error {
	if len(al.AllowMethods) > 0 && len(al.DisallowMethods) > 0 {
		return fmt.Errorf("allow and disallow method sets cannot both be set")
	}
	return nil
}
