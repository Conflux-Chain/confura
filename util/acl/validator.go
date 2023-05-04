package acl

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
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

	// The RPC mothods bonded to be validated against contract address allowlist
	ctAddrAclRpcMethods = map[string]bool{
		// evm space RPC methods
		`eth_call`:                true,
		`eth_estimateGas`:         true,
		`eth_getLogs`:             true,
		`eth_getBalance`:          true,
		`eth_getCode`:             true,
		`eth_getStorageAt`:        true,
		`eth_getTransactionCount`: true,
		// core space RPC methods
		`cfx_call`:                     true,
		`cfx_estimateGasAndCollateral`: true,
		`cfx_getLogs`:                  true,
		`cfx_getBalance`:               true,
		`cfx_getCode`:                  true,
		`cfx_getStorageAt`:             true,
		`cfx_getNextNonce`:             true,
	}
)

// validation context
type Context struct {
	context.Context
	RpcMethod        string
	ExtractRpcParams func() ([]interface{}, error)
}

// allowlist validator. Each allowlist type is "AND"ed together,
// while multiple entries of the same type are "OR"ed.
type Validator struct {
	*AllowList

	// wildchar pattern matching
	allowMethodRules    []string // allow methods
	disallowMethodRules []string // disallow methods
	originRules         []string // request origins

	// contract addresses mapset
	contractAddrRules map[string]bool
}

func NewValidator(al *AllowList) *Validator {
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

	return &Validator{
		AllowList:           al,
		originRules:         originRules,
		allowMethodRules:    allowMethodRules,
		disallowMethodRules: disallowMethodRules,
		contractAddrRules:   contractAddrRules,
	}
}

func (v *Validator) Validate(ctx Context) error {
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
func (v *Validator) validateOrigin(ctx Context) error {
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
func (v *Validator) validateUserAgents(ctx Context) error {
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
func (v *Validator) validateAllowMethods(ctx Context) error {
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

// For EVM space, the following RPC methods take contract address parameter and
// are compatible with this type of allowlisting:
// `eth_call`
// `eth_estimateGas`
// `eth_getLogs`
// `eth_getBalance`
// `eth_getCode`
// `eth_getStorageAt`
// `eth_getTransactionCount`
//
// For core space, with the following RPC methods:
// `cfx_call`
// `cfx_estimateGasAndCollateral`
// `cfx_getLogs`
// `cfx_getBalance`
// `cfx_getCode`
// `cfx_getStorageAt`
// `cfx_getNextNonce`
func (v *Validator) validateContractAddresses(ctx Context) error {
	if len(v.ContractAddresses) == 0 {
		return nil
	}

	if !ctAddrAclRpcMethods[ctx.RpcMethod] {
		// not in the restricted RPC method list
		return nil
	}

	if ctx.ExtractRpcParams == nil {
		return nil
	}

	inputParams, err := ctx.ExtractRpcParams()
	if err != nil || len(inputParams) == 0 {
		return errBadRpcParams
	}

	// extract contract addresses from input params
	var reqContractAddrs []string
	var exctOk bool

	switch ctx.RpcMethod {
	case "eth_call", "eth_estimateGas":
		reqContractAddrs, exctOk = v.extractByEthCallRequest(inputParams[0])
	case "eth_getLogs":
		reqContractAddrs, exctOk = v.extractByEthFilterQuery(inputParams[0])
	case "eth_getBalance", "eth_getTransactionCount":
		reqContractAddrs, exctOk = v.extractByEthAddr(inputParams[0])
	case "eth_getCode", "eth_getStorageAt":
		reqContractAddrs, exctOk = v.extractByEthAddr(inputParams[0])
	case "cfx_call", "cfx_estimateGasAndCollateral":
		reqContractAddrs, exctOk = v.extractByCfxCallRequest(inputParams[0])
	case "cfx_getLogs":
		reqContractAddrs, exctOk = v.extractByCfxLogFilter(inputParams[0])
	case "cfx_getBalance", "cfx_getNextNonce":
		reqContractAddrs, exctOk = v.extractByCfxAddr(inputParams[0])
	case "cfx_getCode", "cfx_getStorageAt":
		reqContractAddrs, exctOk = v.extractByCfxAddr(inputParams[0])
	default:
		logrus.WithFields(logrus.Fields{
			"reqRpcMethod": ctx.RpcMethod,
			"inputParams":  inputParams,
		}).Warn("No method provided to extract contract addresses from input params")
	}

	if !exctOk {
		return errBadRpcParams
	}

	for _, caddr := range reqContractAddrs {
		if !v.contractAddrRules[strings.ToLower(caddr)] {
			return errInvalidContractAddr
		}
	}

	return nil
}

func (v *Validator) extractByCfxCallRequest(param interface{}) (res []string, ok bool) {
	cr, ok := param.(cfxTypes.CallRequest)
	if ok {
		res = append(res, cr.To.String())
	}

	return
}

func (v *Validator) extractByCfxLogFilter(param interface{}) (res []string, ok bool) {
	fq, ok := param.(cfxTypes.LogFilter)
	if !ok {
		return
	}

	for i := range fq.Address {
		res = append(res, fq.Address[i].String())
	}

	return
}

func (v *Validator) extractByCfxAddr(param interface{}) (res []string, ok bool) {
	ca, ok := param.(cfxTypes.Address)
	if ok {
		res = append(res, ca.String())
	}

	return
}

func (v *Validator) extractByEthCallRequest(param interface{}) (res []string, ok bool) {
	cr, ok := param.(web3Types.CallRequest)
	if ok {
		res = append(res, cr.To.String())
	}

	return
}

func (v *Validator) extractByEthFilterQuery(param interface{}) (res []string, ok bool) {
	fq, ok := param.(web3Types.FilterQuery)
	if !ok {
		return
	}

	for i := range fq.Addresses {
		res = append(res, fq.Addresses[i].String())
	}

	return
}

func (v *Validator) extractByEthAddr(param interface{}) (res []string, ok bool) {
	ca, ok := param.(common.Address)
	if ok {
		res = append(res, ca.String())
	}

	return
}
