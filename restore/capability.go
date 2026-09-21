package restore

import "fmt"

// Feature names used in capability checks and blocker evidence.
const (
	FeatureFunctions             = "functions"
	FeatureStreamEntriesRead     = "stream-entriesread"
	FeatureXGroupCreateConsumer  = "xgroup-createconsumer"
	FeatureHashFieldExpiration   = "hash-field-expiration"
	FeatureXClaim                = "xclaim"
)

// CapabilityProfile describes the Redis version a plan targets. Commands
// that require a newer server are either replaced by a supported
// alternative or reported as blockers with evidence.
type CapabilityProfile struct {
	Name  string `json:"name"`
	Major int    `json:"major"`
	Minor int    `json:"minor"`
}

// RedisVersion returns a capability profile for a redis version.
func RedisVersion(major, minor int) CapabilityProfile {
	return CapabilityProfile{
		Name:  fmt.Sprintf("redis-%d.%d", major, minor),
		Major: major,
		Minor: minor,
	}
}

// DefaultProfile returns the profile used when none is configured. It
// matches the newest feature set this library understands.
func DefaultProfile() CapabilityProfile {
	return RedisVersion(7, 4)
}

// Version renders the profile as "major.minor".
func (c CapabilityProfile) Version() string {
	return fmt.Sprintf("%d.%d", c.Major, c.Minor)
}

// featureMinVersion maps feature names to the minimum redis version.
var featureMinVersion = map[string][2]int{
	FeatureFunctions:            {7, 0},
	FeatureStreamEntriesRead:    {7, 0},
	FeatureXGroupCreateConsumer: {6, 2},
	FeatureHashFieldExpiration:  {7, 4},
	FeatureXClaim:               {5, 0},
}

// Supports reports whether the profile supports the named feature.
func (c CapabilityProfile) Supports(feature string) bool {
	min, ok := featureMinVersion[feature]
	if !ok {
		return false
	}
	if c.Major != min[0] {
		return c.Major > min[0]
	}
	return c.Minor >= min[1]
}

// MinVersion returns the minimum redis version for a feature as "x.y".
func MinVersion(feature string) string {
	min, ok := featureMinVersion[feature]
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%d.%d", min[0], min[1])
}
