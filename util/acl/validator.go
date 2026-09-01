package acl

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

var (
	errInvalidOrigin       = errors.New("invalid request origin")
	errInvalidUserAgent    = errors.New("invalid user agent")
	errInvalidContractAddr = errors.New("invalid contract address")
	errBadRpcMethod        = errors.New("bad request method")
	errBadRpcParams        = errors.New("bad RPC parameters")
)

// validation context
type Context struct {
	context.Context

	RpcMethod        string
	ExtractRpcParams func() ([]interface{}, error)
}

type ValidatorFactory func(al *AllowList) Validator

type Validator interface {
	Validate(ctx Context) error
}

// contractAddressParser extracts contract addresses from RPC parameters.
type contractAddressParser func(params []interface{}) ([]string, bool)

type contractAddressProvider interface {
	ContractAddress() (string, bool)
}

// allowlist validator. Each allowlist type is "AND"ed together,
// while multiple entries of the same type are "OR"ed.
type validatorBase struct {
	*AllowList

	// wildchar pattern matching
	allowMethodRules    []string // allow methods
	disallowMethodRules []string // disallow methods
	originRules         []string // request origins

	// contract addresses mapset
	contractAddressRules map[string]bool

	// contract address parsers by RPC method params:
	// RPC method => contractAddressParser
	contractAddressParsers map[string]contractAddressParser
}

func newValidatorBase(al *AllowList) *validatorBase {
	var originRules []string
	for _, r := range al.Origins {
		regp := util.WildCardToRegexp(strings.ToLower(r))
		originRules = append(originRules, regp)
	}

	var allowMethodRules []string
	for _, r := range al.AllowMethods {
		regp := util.WildCardToRegexp(r)
		allowMethodRules = append(allowMethodRules, regp)
	}

	var disallowMethodRules []string
	for _, r := range al.DisallowMethods {
		regp := util.WildCardToRegexp(r)
		disallowMethodRules = append(disallowMethodRules, regp)
	}

	contractAddrRules := make(map[string]bool)
	for _, r := range al.ContractAddresses {
		contractAddrRules[strings.ToLower(r)] = true
	}

	return &validatorBase{
		AllowList:            al,
		originRules:          originRules,
		allowMethodRules:     allowMethodRules,
		disallowMethodRules:  disallowMethodRules,
		contractAddressRules: contractAddrRules,
	}
}

func (v *validatorBase) Validate(ctx Context) error {
	if err := v.validateOrigin(ctx); err != nil {
		return err
	}

	if err := v.validateUserAgents(ctx); err != nil {
		return err
	}

	if err := v.validateAllowMethods(ctx); err != nil {
		return err
	}

	if err := v.validateContractAddresses(ctx); err != nil {
		return err
	}

	return nil
}

// The allowlist of originating domain, which supports wildcard subdomain patterns,
// and the scheme is optional.
func (v *validatorBase) validateOrigin(ctx Context) error {
	if len(v.Origins) == 0 {
		return nil
	}

	reqOrigin, ok := handlers.GetRequestOriginFromContext(ctx)
	if !ok {
		return errInvalidOrigin
	}

	reqOrigin = strings.ToLower(reqOrigin)
	for _, r := range v.originRules {
		matched, err := regexp.MatchString(r, reqOrigin)
		if err == nil && matched {
			return nil
		}
	}

	return errInvalidOrigin
}

// The allowlist of user agent utilizes partial string matching. If the allowlisted
// string is present in the request's full `User-Agent`, it is registered as a match.
func (v *validatorBase) validateUserAgents(ctx Context) error {
	if len(v.UserAgents) == 0 {
		return nil
	}

	reqUA, ok := handlers.GetUserAgentFromContext(ctx)
	if !ok {
		return errInvalidUserAgent
	}

	reqUA = strings.ToLower(reqUA)
	for _, r := range v.UserAgents {
		if strings.Contains(reqUA, strings.ToLower(r)) {
			return nil
		}
	}

	return errInvalidUserAgent
}

// If the allow list is empty, any method calls specified in the disallow list are rejected.
func (v *validatorBase) validateAllowMethods(ctx Context) error {
	if len(v.AllowMethods) == 0 && len(v.DisallowMethods) == 0 {
		return nil
	}

	for _, r := range v.allowMethodRules {
		matched, err := regexp.MatchString(r, ctx.RpcMethod)
		if err == nil && matched {
			return nil
		}
	}

	if len(v.AllowMethods) > 0 {
		return errBadRpcMethod
	}

	for _, r := range v.disallowMethodRules {
		matched, err := regexp.MatchString(r, ctx.RpcMethod)
		if err == nil && matched {
			return errBadRpcMethod
		}
	}

	return nil
}

// Validate the RPC methods which take contract address parameter and
// check if they are compatible with this type of allowlisting.
func (v *validatorBase) validateContractAddresses(ctx Context) error {
	if len(v.ContractAddresses) == 0 {
		return nil
	}

	if ctx.ExtractRpcParams == nil {
		return nil
	}

	parseContractAddresses, ok := v.contractAddressParsers[ctx.RpcMethod]
	if !ok {
		return nil
	}

	inputParams, err := ctx.ExtractRpcParams()
	if err != nil {
		return errBadRpcParams
	}

	contractAddresses, ok := parseContractAddresses(inputParams)
	if !ok {
		return errBadRpcParams
	}

	for _, address := range contractAddresses {
		if !v.contractAddressRules[strings.ToLower(address)] {
			return errInvalidContractAddr
		}
	}

	return nil
}

type EthValidator struct {
	*validatorBase
}

func NewEthValidator(al *AllowList) Validator {
	v := &EthValidator{
		validatorBase: newValidatorBase(al),
	}

	v.contractAddressParsers = map[string]contractAddressParser{
		"eth_call":                        v.parseCallRequest,
		"eth_estimateGas":                 v.parseCallRequest,
		"eth_getLogs":                     v.parseFilterQuery,
		"eth_scanLogs":                    parseContractAddressProvider,
		"eth_scanLogsWithPivotAssumption": parseContractAddressProvider,
		"eth_getBalance":                  v.parseAddr,
		"eth_getTransactionCount":         v.parseAddr,
		"eth_getCode":                     v.parseAddr,
		"eth_getStorageAt":                v.parseAddr,
	}

	for _, address := range v.ContractAddresses {
		if !common.IsHexAddress(address) {
			logrus.WithField("contractAddr", address).Warn("Invalid contract address for allowlist")
			delete(v.contractAddressRules, strings.ToLower(address))
		}
	}

	return v
}

func (v *EthValidator) parseCallRequest(params []interface{}) (res []string, ok bool) {
	if len(params) == 0 {
		return
	}

	cr, ok := params[0].(web3Types.CallRequest)
	if ok {
		res = append(res, cr.To.String())
	}

	return
}

func (v *EthValidator) parseFilterQuery(params []interface{}) (res []string, ok bool) {
	if len(params) == 0 {
		return
	}

	fq, ok := params[0].(web3Types.FilterQuery)
	if ok {
		for i := range fq.Addresses {
			res = append(res, fq.Addresses[i].String())
		}
	}

	return
}

func parseContractAddressProvider(params []interface{}) ([]string, bool) {
	if len(params) == 0 {
		return nil, false
	}

	req, ok := params[0].(contractAddressProvider)
	if !ok {
		return nil, false
	}
	if address, exists := req.ContractAddress(); exists {
		return []string{address}, true
	}
	return nil, true
}

func (v *EthValidator) parseAddr(params []interface{}) (res []string, ok bool) {
	if len(params) == 0 {
		return
	}

	ca, ok := params[0].(common.Address)
	if ok {
		res = append(res, ca.String())
	}

	return
}

type CfxValidator struct {
	*validatorBase
}

func NewCfxValidator(al *AllowList) Validator {
	v := &CfxValidator{
		validatorBase: newValidatorBase(al),
	}

	v.contractAddressParsers = map[string]contractAddressParser{
		"cfx_call":                        v.parseCallRequest,
		"cfx_estimateGasAndCollateral":    v.parseCallRequest,
		"cfx_getLogs":                     v.parseLogFilter,
		"cfx_scanLogs":                    parseContractAddressProvider,
		"cfx_scanLogsWithPivotAssumption": parseContractAddressProvider,
		"cfx_getBalance":                  v.parseAddr,
		"cfx_getNextNonce":                v.parseAddr,
		"cfx_getCode":                     v.parseAddr,
		"cfx_getStorageAt":                v.parseAddr,
	}

	v.uniformContractAddrRulesets()
	return v
}

func (v *CfxValidator) uniformContractAddrRulesets() {
	v.contractAddressRules = make(map[string]bool)

	for _, address := range v.ContractAddresses {
		addr, err := cfxaddress.NewFromBase32(address)
		if err != nil {
			logrus.WithField("contractAddr", address).Warn("Invalid contract address for allowlist")
			continue
		}

		v.contractAddressRules[addr.MustGetBase32Address()] = true
	}
}

func (v *CfxValidator) parseCallRequest(params []interface{}) (res []string, ok bool) {
	if len(params) == 0 {
		return
	}

	cr, ok := params[0].(cfxTypes.CallRequest)
	if ok { // only non-verbose base32 address format is accepted
		res = append(res, cr.To.MustGetBase32Address())
	}

	return
}

func (v *CfxValidator) parseLogFilter(params []interface{}) (res []string, ok bool) {
	if len(params) == 0 {
		return
	}

	fq, ok := params[0].(cfxTypes.LogFilter)
	if ok {
		for i := range fq.Address {
			res = append(res, fq.Address[i].MustGetBase32Address())
		}
	}

	return
}

func (v *CfxValidator) parseAddr(params []interface{}) (res []string, ok bool) {
	if len(params) == 0 {
		return
	}

	ca, ok := params[0].(cfxTypes.Address)
	if ok {
		res = append(res, ca.MustGetVerboseBase32Address())
	}

	return
}
