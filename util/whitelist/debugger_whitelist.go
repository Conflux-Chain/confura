package rpc

import (
	"context"
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

	"github.com/scroll-tech/rpc-gateway/util/rpc/handlers"
)

var (
	whiteListURL   string
	whiteListCache = cache.New(5*time.Minute, 5*time.Minute)
)

func init() {
	whiteListURL = os.Getenv("WHITELIST_BACKEND_URL")
	logrus.Info("whiteListURL: ", whiteListURL)
}

func remoteAddrFromContext(ctx context.Context) string {
	if ip, ok := handlers.GetIPAddressFromContext(ctx); ok {
		return ip
	}

	return "unknown_ip"
}

func IsIPValid() (bool, error) {
	// in dev environment
	if whiteListURL == "" {
		return true, nil
	}

	ip := remoteAddrFromContext(ctx)
	ip = strings.ToLower(ip)

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
