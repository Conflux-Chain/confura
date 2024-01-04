package handlers

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// Remote IP Address with Go:
// https://husobee.github.io/golang/ip-address/2015/12/17/remote-ip-go.html

// ipRange - a structure that holds the start and end of a range of ip addresses
type ipRange struct {
	start net.IP
	end   net.IP
}

// inRange - check to see if a given ip address is within a range given
func inRange(r ipRange, ipAddress net.IP) bool {
	// strcmp type byte comparison
	if bytes.Compare(ipAddress, r.start) >= 0 && bytes.Compare(ipAddress, r.end) < 0 {
		return true
	}
	return false
}

var privateRanges = []ipRange{
	{
		start: net.ParseIP("10.0.0.0"),
		end:   net.ParseIP("10.255.255.255"),
	},
	{
		start: net.ParseIP("100.64.0.0"),
		end:   net.ParseIP("100.127.255.255"),
	},
	{
		start: net.ParseIP("172.16.0.0"),
		end:   net.ParseIP("172.31.255.255"),
	},
	{
		start: net.ParseIP("192.0.0.0"),
		end:   net.ParseIP("192.0.0.255"),
	},
	{
		start: net.ParseIP("192.168.0.0"),
		end:   net.ParseIP("192.168.255.255"),
	},
	{
		start: net.ParseIP("198.18.0.0"),
		end:   net.ParseIP("198.19.255.255"),
	},
}

// isPrivateSubnet - check to see if this ip is in a private subnet
func isPrivateSubnet(ipAddress net.IP) bool {
	// my use case is only concerned with ipv4 atm
	if ipCheck := ipAddress.To4(); ipCheck != nil {
		// iterate over all our ranges
		for _, r := range privateRanges {
			// check if this ip is in a private range
			if inRange(r, ipAddress) {
				return true
			}
		}
	}
	return false
}

// GetIPAddress returns the remote IP address.
func GetIPAddress(r *http.Request) string {
	for _, h := range []string{"X-Forwarded-For", "X-Real-Ip"} {
		addresses := strings.Split(r.Header.Get(h), ",")
		// march from right to left until we get a public address
		// that will be the address right before our proxy.
		for i := len(addresses) - 1; i >= 0; i-- {
			ip := strings.TrimSpace(addresses[i])
			// header can contain spaces too, strip those out.
			realIP := net.ParseIP(ip)
			if !realIP.IsGlobalUnicast() || isPrivateSubnet(realIP) {
				// bad address, go to next
				continue
			}
			return ip
		}
	}

	if idx := strings.Index(r.RemoteAddr, ":"); idx != -1 {
		return r.RemoteAddr[:idx]
	}

	return r.RemoteAddr
}

func GetIPAddressFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(CtxKeyRealIP).(string)
	return val, ok
}

func GetAccessToken(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}

	// Access token can be passed in with the following two ways:
	// Appended after the root path with pattern: http://example.com/${accessToken}.
	key := strings.TrimLeft(r.URL.EscapedPath(), "/")
	if idx := strings.Index(key, "/"); idx > 0 {
		key = key[:idx]
	}

	if len(key) > 0 {
		ekey, err := url.PathUnescape(key)
		if err == nil {
			return ekey
		}
	}

	// Or attached in the HTTP header with the key of "Access-Token".
	return r.Header.Get("Access-Token")
}

func GetAccessTokenFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(CtxKeyAccessToken).(string)
	return val, ok
}

func GetRequestOriginFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(CtxKeyReqOrigin).(string)
	return val, ok
}

func GetUserAgentFromContext(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(CtxKeyUserAgent).(string)
	return val, ok
}

func GetAuthIdFromContext(ctx context.Context) (string, bool) {
	authId, ok := ctx.Value(CtxKeyAuthId).(string)
	return authId, ok
}
