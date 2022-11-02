package whitelist

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

var (
	whiteListURL   string
	whiteListCache = cache.New(5*time.Minute, 5*time.Minute)
)

func init() {
	whiteListURL = os.Getenv("WHITELIST_BACKEND_URL")
	logrus.Info("whiteListURL: ", whiteListURL)
}

// IsIPValid checks if the debugger IP is in the whitelist through the whitelist backend
func IsIPValid(req *http.Request) (bool, error) {
	// in dev environment
	if whiteListURL == "" {
		return true, nil
	}

	ip := GetIPFromRequestEnv(req)
	logrus.Info(ip)
	if ip == "unknown_ip" {
		return false, nil
	}

	cache_key := "whitelist-ip-" + ip
	valid, found := whiteListCache.Get(cache_key)
	logrus.Debug("whitelist IP cache Get ip: ", ip, ", found: ", found, ", valid: ", valid)
	if found {
		return valid.(bool), nil
	}

	debuggerListURL, err := url.JoinPath(whiteListURL, "api/get_debugger")
	if err != nil {
		return false, err
	}

	params := url.Values{}
	Url, err := url.Parse(debuggerListURL)
	if err != nil {
		return false, err
	}

	params.Set("accept", "application/json")
	params.Set("ip", ip)
	Url.RawQuery = params.Encode()
	urlPath := Url.String()
	resp, err := http.Get(urlPath)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return false, err
	}

	debuggerList, ok := data["debugger"]
	if !ok {
		return false, errors.New("Parse Json fail, no debugger")
	}
	ok = debuggerList != nil
	whiteListCache.Set(cache_key, ok, cache.DefaultExpiration)
	return ok, nil
}

func GetIPFromRequestEnv(req *http.Request) string {
	fwdAddress := req.Header.Get("X-Forwarded-For")
	if fwdAddress != "" {
		return strings.ToLower(strings.Split(fwdAddress, ", ")[0])
	}
	ip := req.Header.Get("X-Real-IP")
	if ip != "" {
		return strings.ToLower(ip)
	}
	ip = strings.Split(req.RemoteAddr, ":")[0]
	if ip != "" {
		return strings.ToLower(ip)
	}
	return "unknown_ip"
}
