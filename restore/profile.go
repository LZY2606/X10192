package restore

import "fmt"

// Feature is a Redis capability a restore step may depend on.
type Feature string

const (
	// FeatureFunctionLoad is FUNCTION LOAD (Redis 7.0).
	FeatureFunctionLoad Feature = "function-load"
	// FeatureHashFieldExpire is HPEXPIREAT/HPERSIST (Redis 7.4).
	FeatureHashFieldExpire Feature = "hash-field-expire"
	// FeatureStreamCreateConsumer is XGROUP CREATECONSUMER (Redis 6.2).
	FeatureStreamCreateConsumer Feature = "stream-create-consumer"
	// FeatureStreamEntriesRead is XGROUP CREATE ... ENTRIESREAD (Redis 7.0).
	FeatureStreamEntriesRead Feature = "stream-entries-read"
	// FeatureStreamMetaExtended is XSETID ... ENTRIESADDED/MAXDELETEDID (Redis 7.0).
	FeatureStreamMetaExtended Feature = "stream-xsetid-extended"
)

// featureMinVersion records the first Redis version supporting a feature.
var featureMinVersion = map[Feature][2]int{
	FeatureFunctionLoad:         {7, 0},
	FeatureHashFieldExpire:      {7, 4},
	FeatureStreamCreateConsumer: {6, 2},
	FeatureStreamEntriesRead:    {7, 0},
	FeatureStreamMetaExtended:   {7, 0},
}

// Profile describes the capabilities of the target Redis deployment. The
// planner uses it to pick fallback commands or to raise evidenced blockers.
type Profile struct {
	Name         string `json:"name"`
	RedisVersion string `json:"redisVersion"`
	major        int
	minor        int
}

// Predefined capability profiles for common Redis versions.
var (
	ProfileRedis50 = &Profile{Name: "redis-5.0", RedisVersion: "5.0", major: 5, minor: 0}
	ProfileRedis62 = &Profile{Name: "redis-6.2", RedisVersion: "6.2", major: 6, minor: 2}
	ProfileRedis70 = &Profile{Name: "redis-7.0", RedisVersion: "7.0", major: 7, minor: 0}
	ProfileRedis72 = &Profile{Name: "redis-7.2", RedisVersion: "7.2", major: 7, minor: 2}
	// ProfileRedis74 is the default profile.
	ProfileRedis74 = &Profile{Name: "redis-7.4", RedisVersion: "7.4", major: 7, minor: 4}
)

// Supports reports whether the profile supports the feature.
func (p *Profile) Supports(f Feature) bool {
	min, ok := featureMinVersion[f]
	if !ok {
		return true
	}
	return p.major > min[0] || (p.major == min[0] && p.minor >= min[1])
}

// Evidence returns a human-readable explanation of why the feature is not
// supported by the profile. It is only meaningful when Supports is false.
func (p *Profile) Evidence(f Feature) string {
	min := featureMinVersion[f]
	return fmt.Sprintf("feature %q requires Redis >= %d.%d, target profile %q provides Redis %s",
		f, min[0], min[1], p.Name, p.RedisVersion)
}
