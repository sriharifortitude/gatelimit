// Package keys turns a request into the string a limit is keyed on.
package keys

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// APIKey returns a stable, non-reversible key for the credential on the
// request, or "" if there is none. The credential is hashed because keys
// end up in Redis and in logs, and neither should hold something that
// authenticates to the upstream.
func APIKey(r *http.Request) string {
	credential := r.Header.Get("X-API-Key")
	if credential == "" {
		if auth := r.Header.Get("Authorization"); len(auth) > 7 && strings.EqualFold(auth[:7], "bearer ") {
			credential = strings.TrimSpace(auth[7:])
		}
	}
	if credential == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(credential))
	return "k:" + hex.EncodeToString(sum[:16])
}

// ClientIP returns the client's address. X-Forwarded-For is believed only
// when the immediate peer is a trusted proxy, and then the rightmost
// address not added by a trusted proxy is taken -- the one the last trusted
// hop saw -- because everything to its left was supplied by the client.
func ClientIP(r *http.Request, trusted []netip.Prefix) string {
	peer := remoteAddr(r)
	if !isTrusted(peer, trusted) {
		return "ip:" + peer.String()
	}
	forwarded := r.Header.Values("X-Forwarded-For")
	var hops []string
	for _, v := range forwarded {
		for _, part := range strings.Split(v, ",") {
			if p := strings.TrimSpace(part); p != "" {
				hops = append(hops, p)
			}
		}
	}
	for i := len(hops) - 1; i >= 0; i-- {
		addr, err := netip.ParseAddr(hops[i])
		if err != nil {
			break
		}
		if !isTrusted(addr, trusted) {
			return "ip:" + addr.String()
		}
	}
	return "ip:" + peer.String()
}

func remoteAddr(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return addr.Unmap()
}

func isTrusted(addr netip.Addr, trusted []netip.Prefix) bool {
	if !addr.IsValid() {
		return false
	}
	for _, p := range trusted {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}
