package whitelist

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

var (
	whiteListURL   string
	whiteListCache = cache.New(5*time.Minute, 5*time.Minute)
	proxyCount     int
)

func init() {
	whiteListURL = os.Getenv("WHITELIST_BACKEND_URL")
	envProxyCount, err := strconv.Atoi(os.Getenv("PROXY_COUNT"))
	if err != nil || envProxyCount < 0 {
		envProxyCount = 0 // fetch RemoteAddr
	}
	proxyCount = envProxyCount
	logrus.Info("whiteListURL: ", whiteListURL, ", proxyCount: ", proxyCount)
}

// IsIPValid checks if the debugger IP is in the whitelist through the whitelist backend
func IsIPValid(ip string) bool {
	if whiteListURL == "" {
		return true
	}
	ip = strings.ToLower(ip)
	cacheKey := "whitelist-ip-" + ip
	cacheValue, found := whiteListCache.Get(cacheKey)
	logrus.Debug("whitelist IP cache Get ip: ", ip, ", found: ", found, ", cacheValue: ", cacheValue)
	if found {
		return cacheValue.(bool)
	}

	params := url.Values{}
	getDebuggerURL, err := url.Parse(whiteListURL + "/api/get_debugger")
	logrus.Debug("getDebuggerURL: ", getDebuggerURL)
	if err != nil {
		logrus.Error(err)
		return false
	}
	params.Set("accept", "application/json")
	params.Set("ip", ip)
	getDebuggerURL.RawQuery = params.Encode()
	resp, err := http.Get(getDebuggerURL.String())
	if err != nil {
		logrus.Error(err)
		return false
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		logrus.Error(err)
		return false
	}

	debuggerList, ok := data["debugger"]
	isValid := (ok == true) && (debuggerList != nil)
	whiteListCache.Set(cacheKey, isValid, cache.DefaultExpiration)
	return isValid
}

// GetInvalidIPErrorMsg return `Method not supported` (code=-32004) if invalid. Reference:
// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-1474.md
func GetInvalidIPErrorMsg(reqByte []byte) ([]byte, error) {
	var reqs []ReqPayload // for batch request
	var req ReqPayload    // for single request
	if isBatchRequest(reqByte, &reqs) {
		var resps []InvalidIPRespPayload
		for _, req := range reqs {
			var resp InvalidIPRespPayload
			resp.ID = req.ID
			resp.JsonRPC = req.JsonRPC
			resp.Method = req.Method
			resp.Error.Code = -32004
			resp.Error.Message = "Operation not permitted"
			resps = append(resps, resp)
		}
		return json.Marshal(resps)
	} else if isSingleRequest(reqByte, &req) {
		var resp InvalidIPRespPayload
		resp.ID = req.ID
		resp.JsonRPC = req.JsonRPC
		resp.Method = req.Method
		resp.Error.Code = -32004
		resp.Error.Message = "Operation not permitted"
		return json.Marshal(resp)
	}
	logrus.Debug("invalid request reqByte: ", string(reqByte))
	return []byte("invalid request " + string(reqByte)), nil
}

type ReqPayload struct {
	ID      interface{} `json:"id,omitempty"`
	JsonRPC string      `json:"jsonrpc,omitempty"`
	Method  string      `json:"method,omitempty"`
}

type InvalidIPRespPayload struct {
	ID      interface{}     `json:"id,omitempty"`
	JsonRPC string          `json:"jsonrpc,omitempty"`
	Method  string          `json:"method,omitempty"`
	Error   InvalidIPErrMsg `json:"error,omitempty"`
}

type InvalidIPErrMsg struct {
	Code    int64  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func isBatchRequest(reqByte []byte, reqs *[]ReqPayload) bool {
	err := json.Unmarshal(reqByte, reqs)
	return err == nil
}

func isSingleRequest(reqByte []byte, req *ReqPayload) bool {
	err := json.Unmarshal(reqByte, req)
	return err == nil
}

func GetClientIPFromRequest(r *http.Request) string {
	logrus.Debug("proxyCount: ", proxyCount)
	if proxyCount > 0 {
		xForwardedFor := r.Header.Get("X-Forwarded-For")
		if xForwardedFor != "" {
			xForwardedForParts := strings.Split(xForwardedFor, ",")
			// Avoid reading the user's forged request header by configuring the count of reverse proxies
			partIndex := len(xForwardedForParts) - proxyCount
			if partIndex < 0 {
				partIndex = 0
			}
			return strings.TrimSpace(xForwardedForParts[partIndex])
		}
	}

	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteIP = r.RemoteAddr
	}
	return remoteIP
}
