package restore

import (
	"fmt"
	"strconv"
	"strings"
)

// Feature names a Redis command capability that may not exist on every server
// version. Versions reference the Redis open-source release train.
const (
	// FeaturePEXPIREAT: PEXPIREAT with absolute millisecond deadline (2.6.0).
	FeaturePEXPIREAT = "pexpireat"
	// FeatureHPEXPIREAT: hash field expiration HPEXPIREAT (7.4.0).
	FeatureHPEXPIREAT = "hpexpireat"
	// FeatureFunctions: FUNCTION LOAD[ REPLACE] (7.0.0).
	FeatureFunctions = "functions"
	// FeatureXGroupMKStream: XGROUP CREATE ... MKSTREAM (5.0.0, streams base).
	FeatureXGroupMKStream = "xgroup-mkstream"
	// FeatureXGroupCreateConsumer: XGROUP CREATECONSUMER (7.0.0).
	FeatureXGroupCreateConsumer = "xgroup-createconsumer"
	// FeatureXGroupEntriesRead: XGROUP CREATE/SETID ... ENTRIESREAD (7.0.0).
	FeatureXGroupEntriesRead = "xgroup-entriesread"
	// FeatureXClaimTime: XCLAIM ... TIME ... (6.2.0).
	FeatureXClaimTime = "xclaim-time"
)

// version is a numeric Redis version [major, minor, patch].
type version [3]int

// CapabilityProfile describes the command surface of the target Redis server.
// Plans select commands (or produce evidence-backed blockers) from it.
type CapabilityProfile struct {
	// Name identifies the profile, e.g. "redis-7.4".
	Name string `json:"name"`
	// Version is the semantic server version.
	Version string `json:"version"`
	ver     version
	// featureOverrides optionally pins a feature on/off independent of version.
	// Keys are Feature* constants; it is not serialized and never influences
	// deterministic plan bytes beyond command selection.
	featureOverrides map[string]bool
}

func parseVersion(s string) (version, error) {
	parts := strings.Split(strings.TrimSpace(s), ".")
	if len(parts) > 3 {
		return version{}, fmt.Errorf("invalid redis version: %q", s)
	}
	var v version
	for i, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n < 0 {
			return version{}, fmt.Errorf("invalid redis version: %q", s)
		}
		v[i] = n
	}
	return v, nil
}

func (a version) atLeast(b version) bool {
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return true
}

// NewProfile builds a capability profile for a Redis semantic version.
func NewProfile(name, semanticVersion string) (*CapabilityProfile, error) {
	v, err := parseVersion(semanticVersion)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = "redis-" + semanticVersion
	}
	return &CapabilityProfile{Name: name, Version: semanticVersion, ver: v}, nil
}

// WithFeatureOverride pins a feature to a fixed support value. It returns the
// profile for chaining. The receiver is never mutated: a copy is returned so
// predefined profiles (ProfileRedis60, ...) stay shared and immutable.
func (p *CapabilityProfile) WithFeatureOverride(feature string, supported bool) *CapabilityProfile {
	cp := &CapabilityProfile{
		Name:             p.Name,
		Version:          p.Version,
		ver:              p.ver,
		featureOverrides: make(map[string]bool, len(p.featureOverrides)+1),
	}
	for k, v := range p.featureOverrides {
		cp.featureOverrides[k] = v
	}
	cp.featureOverrides[feature] = supported
	return cp
}

// Supports reports whether the target profile provides the feature.
func (p *CapabilityProfile) Supports(feature string) bool {
	if p == nil {
		p = DefaultProfile
	}
	if supported, ok := p.featureOverrides[feature]; ok {
		return supported
	}
	min, ok := featureMinVersion[feature]
	if !ok {
		return false
	}
	return p.ver.atLeast(min)
}

// featureMinVersion is the evidence table for version-gated commands.
// Sources: Redis command documentation release history:
//   - PEXPIREAT: Redis 2.6.0
//   - HPEXPIREAT (hash field expiration): Redis 7.4.0
//   - FUNCTION LOAD: Redis 7.0.0
//   - XGROUP ... MKSTREAM: Redis 5.0.0 (streams GA)
//   - XGROUP CREATECONSUMER / ENTRIESREAD: Redis 7.0.0
//   - XCLAIM ... TIME ...: Redis 6.2.0
var featureMinVersion = map[string]version{
	FeaturePEXPIREAT:            {2, 6, 0},
	FeatureHPEXPIREAT:           {7, 4, 0},
	FeatureFunctions:            {7, 0, 0},
	FeatureXGroupMKStream:       {5, 0, 0},
	FeatureXGroupCreateConsumer: {7, 0, 0},
	FeatureXGroupEntriesRead:    {7, 0, 0},
	FeatureXClaimTime:           {6, 2, 0},
}

// prebuilt profiles for common targets.
func mustProfile(name, ver string) *CapabilityProfile {
	p, err := NewProfile(name, ver)
	if err != nil {
		panic(err)
	}
	return p
}

// Predefined target profiles.
var (
	ProfileRedis60 = mustProfile("redis-6.0", "6.0.0")
	ProfileRedis62 = mustProfile("redis-6.2", "6.2.0")
	ProfileRedis70 = mustProfile("redis-7.0", "7.0.0")
	ProfileRedis72 = mustProfile("redis-7.2", "7.2.0")
	ProfileRedis74 = mustProfile("redis-7.4", "7.4.0")
)

// DefaultProfile is used when no target is given (Redis 7.4).
var DefaultProfile = ProfileRedis74
