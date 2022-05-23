package node

import "strings"

// Url2NodeName gets node name from url string.
func Url2NodeName(url string) string {
	nodeName := strings.ToLower(url)
	nodeName = strings.TrimPrefix(nodeName, "http://")
	nodeName = strings.TrimPrefix(nodeName, "https://")
	nodeName = strings.TrimPrefix(nodeName, "ws://")
	nodeName = strings.TrimPrefix(nodeName, "wss://")
	return strings.TrimPrefix(nodeName, "/")
}
