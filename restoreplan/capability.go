package restoreplan

// CapabilityProfile describes the restore-relevant command support of a
// target Redis/Valkey build. Custom profiles can be supplied via Options.
type CapabilityProfile struct {
	Name string `json:"name"`
	// Flavor is "redis" or "valkey".
	Flavor string `json:"flavor"`
	// Version semantic major.minor.patch components.
	Major int `json:"major"`
	Minor int `json:"minor"`
	Patch int `json:"patch"`

	// Streams: XADD/XGROUP/XCLAIM (Redis 5.0+).
	Streams bool `json:"streams"`
	// XGroupCreateConsumer: XGROUP CREATECONSUMER (Redis 6.2+).
	XGroupCreateConsumer bool `json:"xgroupCreateConsumer"`
	// XSetIdMetadata: XSETID ENTRIESREAD/MAXDELETEDID (Redis 7.0+).
	XSetIdMetadata bool `json:"xsetidMetadata"`
	// Functions: FUNCTION LOAD (Redis 7.0+).
	Functions bool `json:"functions"`
	// HashFieldExpiry: HPEXPIREAT/HPERSIST (Redis 7.4+).
	HashFieldExpiry bool `json:"hashFieldExpiry"`
	// MKStream on XGROUP CREATE (Redis 6.2+ allows MKSTREAM at creation).
	XGroupMKStream bool `json:"xgroupMkStream"`
}

func profile(flavor string, major, minor, patch int) *CapabilityProfile {
	p := &CapabilityProfile{
		Flavor: flavor,
		Major:  major, Minor: minor, Patch: patch,
		Name: flavor + "-" + itoa(major) + "." + itoa(minor) + "." + itoa(patch),
	}
	atLeast := func(maj, min int) bool {
		return major > maj || (major == maj && minor >= min)
	}
	p.Streams = (flavor == "redis" && atLeast(5, 0))
	p.XGroupCreateConsumer = p.Streams && atLeast(6, 2)
	p.XGroupMKStream = p.Streams && atLeast(6, 2)
	p.XSetIdMetadata = atLeast(7, 0)
	p.Functions = atLeast(7, 0)
	p.HashFieldExpiry = (flavor == "redis" && atLeast(7, 4)) || (flavor == "valkey" && atLeast(9, 0))
	return p
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var neg bool
	if i < 0 {
		neg = true
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// Known capability profiles.
var (
	Redis50  = profile("redis", 5, 0, 0)
	Redis60  = profile("redis", 6, 0, 0)
	Redis62  = profile("redis", 6, 2, 0)
	Redis70  = profile("redis", 7, 0, 0)
	Redis72  = profile("redis", 7, 2, 0)
	Redis74  = profile("redis", 7, 4, 0)
	Valkey80 = profile("valkey", 8, 0, 0)
	Valkey90 = profile("valkey", 9, 0, 0)
)
