package handlers

import (
	"net/http"
	"testing"
)

func TestRemoteIPCanonicalization(t *testing.T) {
	for _, tc := range []struct{ remote, forwarded, want string }{
		{"[2001:db8::1]:1234", "", "2001:db8::1"},
		{"[2001:db8::2]:1234", "", "2001:db8::2"},
		{"2001:db8::1", "", "2001:db8::1"},
		{"192.0.2.1:456", "", "192.0.2.1"},
		{"192.0.2.1:456", "2001:4860:0000:0000:0000:0000:0000:0001", "2001:4860::1"},
		{"192.0.2.1:456", "10.255.255.255", "192.0.2.1"},
	} {
		r := &http.Request{RemoteAddr: tc.remote, Header: make(http.Header)}
		r.Header.Set("X-Forwarded-For", tc.forwarded)
		if got := GetIPAddress(r); got != tc.want {
			t.Errorf("%+v: %q", tc, got)
		}
	}
}
