// Package config loads and validates the JSON configuration.
//
// JSON, not YAML, so the parser is the standard library's and unknown
// fields are a hard error (DisallowUnknownFields). A misspelled "burst"
// that silently became "no burst limit" is the kind of mistake a rate
// limiter must not make.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"
)

type Config struct {
	// Listen is the address the proxy binds, e.g. ":8080".
	Listen string `json:"listen"`
	// Upstream is the base URL requests are forwarded to.
	Upstream string `json:"upstream"`
	// Store is "memory" or "redis".
	Store string `json:"store"`
	// RedisAddrEnv names the environment variable holding host:port. Never the address itself.
	RedisAddrEnv string `json:"redis_addr_env,omitempty"`
	// OnStoreError is "open" (let requests through, count them) or "closed" (503).
	OnStoreError string `json:"on_store_error"`
	// TrustedProxies are CIDRs whose X-Forwarded-For is believed.
	TrustedProxies []string `json:"trusted_proxies,omitempty"`
	// Rules are tried in order; the first whose path prefix matches applies.
	Rules []Rule `json:"rules"`

	trusted []netip.Prefix
}

type Rule struct {
	// Name appears in metrics and logs.
	Name string `json:"name"`
	// PathPrefix selects requests; "/" matches everything.
	PathPrefix string `json:"path_prefix"`
	// Key is "api_key" (X-API-Key or Authorization: Bearer, hashed) or "ip".
	Key string `json:"key"`
	// Algorithm is "token_bucket" or "sliding_window".
	Algorithm string `json:"algorithm"`
	// Rate requests per Period.
	Rate int `json:"rate"`
	// Period as a Go duration string: "1s", "1m", "1h".
	Period string `json:"period"`
	// Burst for token buckets. Defaults to Rate.
	Burst int `json:"burst,omitempty"`

	period time.Duration
}

// Period returns the parsed period.
func (r Rule) PeriodDuration() time.Duration { return r.period }

// Trusted returns the parsed trusted-proxy prefixes.
func (c Config) Trusted() []netip.Prefix { return c.trusted }

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

func Parse(r io.Reader) (*Config, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

func (c *Config) validate() error {
	var errs []error
	if c.Listen == "" {
		errs = append(errs, errors.New("listen: required"))
	}
	if u, err := url.Parse(c.Upstream); err != nil || u.Scheme == "" || u.Host == "" {
		errs = append(errs, errors.New("upstream: must be an absolute http(s) URL"))
	}
	switch c.Store {
	case "memory":
		if c.RedisAddrEnv != "" {
			errs = append(errs, errors.New("redis_addr_env: only valid with store \"redis\""))
		}
	case "redis":
		if c.RedisAddrEnv == "" {
			errs = append(errs, errors.New("redis_addr_env: required with store \"redis\""))
		}
	default:
		errs = append(errs, errors.New("store: must be \"memory\" or \"redis\""))
	}
	if c.OnStoreError != "open" && c.OnStoreError != "closed" {
		errs = append(errs, errors.New("on_store_error: must be \"open\" or \"closed\""))
	}
	for _, cidr := range c.TrustedProxies {
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			errs = append(errs, fmt.Errorf("trusted_proxies: %q is not a CIDR", cidr))
			continue
		}
		c.trusted = append(c.trusted, p)
	}
	if len(c.Rules) == 0 {
		errs = append(errs, errors.New("rules: at least one rule is required"))
	}
	seen := map[string]bool{}
	for i := range c.Rules {
		r := &c.Rules[i]
		at := fmt.Sprintf("rules[%d]", i)
		if r.Name == "" {
			errs = append(errs, fmt.Errorf("%s.name: required", at))
		} else if seen[r.Name] {
			errs = append(errs, fmt.Errorf("%s.name: %q is used twice", at, r.Name))
		}
		seen[r.Name] = true
		if !strings.HasPrefix(r.PathPrefix, "/") {
			errs = append(errs, fmt.Errorf("%s.path_prefix: must start with /", at))
		}
		if r.Key != "api_key" && r.Key != "ip" {
			errs = append(errs, fmt.Errorf("%s.key: must be \"api_key\" or \"ip\"", at))
		}
		if r.Algorithm != "token_bucket" && r.Algorithm != "sliding_window" {
			errs = append(errs, fmt.Errorf("%s.algorithm: must be \"token_bucket\" or \"sliding_window\"", at))
		}
		if r.Rate <= 0 {
			errs = append(errs, fmt.Errorf("%s.rate: must be positive", at))
		}
		d, err := time.ParseDuration(r.Period)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s.period: must be a positive duration like \"1s\" or \"1h\"", at))
		}
		r.period = d
		if r.Burst < 0 {
			errs = append(errs, fmt.Errorf("%s.burst: must not be negative", at))
		}
		if r.Burst == 0 {
			r.Burst = r.Rate
		}
		if r.Algorithm == "sliding_window" && r.Burst != r.Rate {
			errs = append(errs, fmt.Errorf("%s.burst: not used by sliding_window; remove it", at))
		}
	}
	return errors.Join(errs...)
}
