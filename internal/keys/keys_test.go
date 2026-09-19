package keys

import (
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestAPIKeyIsHashedAndHeaderAgnostic(t *testing.T) {
	a := httptest.NewRequest("GET", "/", nil)
	a.Header.Set("X-API-Key", "secret-1")
	b := httptest.NewRequest("GET", "/", nil)
	b.Header.Set("Authorization", "Bearer secret-1")
	c := httptest.NewRequest("GET", "/", nil)
	c.Header.Set("Authorization", "bearer   secret-1  ")

	ka, kb, kc := APIKey(a), APIKey(b), APIKey(c)
	if ka != kb || kb != kc {
		t.Fatalf("same credential must give one key: %s %s %s", ka, kb, kc)
	}
	if ka == "k:secret-1" || len(ka) != 2+32 {
		t.Fatalf("key must be a hash prefix, got %q", ka)
	}
	// sha256("secret-1") first 16 bytes, computed independently: 4a8c... is not
	// asserted here because the property that matters is non-reversibility and
	// stability, both covered above.
	if APIKey(httptest.NewRequest("GET", "/", nil)) != "" {
		t.Fatal("no credential must give an empty key")
	}
	d := httptest.NewRequest("GET", "/", nil)
	d.Header.Set("Authorization", "Basic abc")
	if APIKey(d) != "" {
		t.Fatal("a non-bearer Authorization is not an API key")
	}
}

func TestClientIP(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	cases := []struct {
		name, remote, xff, want string
	}{
		{"direct client, header ignored", "203.0.113.5:4444", "1.2.3.4", "ip:203.0.113.5"},
		{"via trusted proxy, one hop", "10.0.0.1:80", "198.51.100.7", "ip:198.51.100.7"},
		{"via trusted proxy, client forged an earlier hop", "10.0.0.1:80", "1.1.1.1, 198.51.100.7", "ip:198.51.100.7"},
		{"two trusted proxies in the chain", "10.0.0.1:80", "198.51.100.7, 10.0.0.2", "ip:198.51.100.7"},
		{"trusted proxy but empty header", "10.0.0.1:80", "", "ip:10.0.0.1"},
		{"garbage in the header stops the walk", "10.0.0.1:80", "198.51.100.7, not-an-ip", "ip:10.0.0.1"},
		{"ipv6 client", "[2001:db8::1]:5555", "", "ip:2001:db8::1"},
		{"ipv4-mapped ipv6 is unmapped", "[::ffff:203.0.113.9]:1", "", "ip:203.0.113.9"},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if got := ClientIP(r, trusted); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}
