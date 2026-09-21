// Package restore builds deterministic, serializable restore plans from
// decoded RDB objects. A RestorePlan describes which Redis commands would be
// needed to rebuild the dataset on a target server, without ever connecting
// to or executing against a real Redis instance.
package restore

import (
	"encoding/json"
	"time"

	"github.com/hdt3213/rdb/model"
)

// FormatVersion is the version of the RestorePlan serialization format.
const FormatVersion = 1

// ExpiredPolicy decides how keys whose expiration is before the restore
// reference time are handled.
type ExpiredPolicy string

const (
	// ExpiredPolicySkip drops expired keys from the plan.
	ExpiredPolicySkip ExpiredPolicy = "skip"
	// ExpiredPolicyRetain keeps expired keys in the plan, including their
	// (already past) absolute expiration.
	ExpiredPolicyRetain ExpiredPolicy = "retain"
	// ExpiredPolicyError makes Build fail on the first expired key.
	ExpiredPolicyError ExpiredPolicy = "error"
)

// CapabilityProfile describes the Redis commands a restore target supports.
// The builder uses it to pick alternative commands or to emit blockers with
// evidence when a lossless restore is impossible on the target.
type CapabilityProfile struct {
	Name                string `json:"name"`
	Major               int    `json:"major"`
	Minor               int    `json:"minor"`
	FunctionRestore     bool   `json:"functionRestore"`     // FUNCTION RESTORE (Redis >= 7.0)
	XGroupEntriesRead   bool   `json:"xgroupEntriesRead"`   // XGROUP ... ENTRIESREAD (Redis >= 7.0)
	XGroupCreateConsumer bool  `json:"xgroupCreateConsumer"` // XGROUP CREATECONSUMER (Redis >= 6.2)
	HashFieldExpiration bool   `json:"hashFieldExpiration"` // HEXPIREAT (Redis >= 7.4)
}

// RedisProfile derives a capability profile from a Redis version.
func RedisProfile(major, minor int) CapabilityProfile {
	atLeast := func(maj, min int) bool {
		return major > maj || (major == maj && minor >= min)
	}
	return CapabilityProfile{
		Name:                 "redis-" + itoa(major) + "." + itoa(minor),
		Major:                major,
		Minor:                minor,
		FunctionRestore:      atLeast(7, 0),
		XGroupEntriesRead:    atLeast(7, 0),
		XGroupCreateConsumer: atLeast(6, 2),
		HashFieldExpiration:  atLeast(7, 4),
	}
}

// DefaultProfile returns the profile used when Options.Target is not set:
// a Redis 7.2 compatible target.
func DefaultProfile() CapabilityProfile {
	return RedisProfile(7, 2)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// Options controls plan generation.
type Options struct {
	// ReferenceTime is the restore reference time. Relative TTLs and stream
	// idle times are computed against it, making the plan replayable.
	// It must be non-zero.
	ReferenceTime time.Time
	// ExpiredPolicy decides the fate of keys expired before ReferenceTime.
	// Defaults to ExpiredPolicySkip.
	ExpiredPolicy ExpiredPolicy
	// Target describes the capabilities of the restore target.
	// Defaults to DefaultProfile().
	Target *CapabilityProfile
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.ExpiredPolicy == "" {
		out.ExpiredPolicy = ExpiredPolicySkip
	}
	if out.Target == nil {
		p := DefaultProfile()
		out.Target = &p
	}
	return out
}

// Command is a single Redis command. Args excludes the command name and
// includes the key. Arguments are kept as raw strings; JSON serialization
// escapes non-UTF8 bytes deterministically.
type Command struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
}

// Cmd is a shorthand for building a Command.
func Cmd(name string, args ...string) Command {
	return Command{Name: name, Args: args}
}

// ExpirationSpec describes key expiration in a replayable way: the absolute
// expiration from the RDB plus the TTL relative to the restore reference time.
type ExpirationSpec struct {
	// ExpireAtUnixMs is the absolute expiration time in unix milliseconds.
	ExpireAtUnixMs int64 `json:"expireAtUnixMs"`
	// TTLMsAtReference is ExpireAtUnixMs minus the reference time in ms.
	// Negative means the key was already expired at the reference time.
	TTLMsAtReference int64 `json:"ttlMsAtReference"`
	// ExpiredAtReference reports whether the key was expired at ReferenceTime.
	ExpiredAtReference bool `json:"expiredAtReference"`
	// Command is the command used to apply the expiration (PEXPIREAT).
	// It carries an absolute timestamp so it replays correctly regardless
	// of when the plan is executed.
	Command string `json:"command"`
}

// SourceRef locates the decoded event a plan item originates from.
type SourceRef struct {
	// EventIndex is the index of the object in the decoded event stream
	// passed to Build (0 based).
	EventIndex int `json:"eventIndex"`
	// ObjectID is the stable identity of the source object; it does not
	// depend on the order of the input events.
	ObjectID string `json:"objectId"`
}

// AtomicitySpec marks items that belong to one logical object whose commands
// must not be interleaved with writes to the same key by other actors.
type AtomicitySpec struct {
	// Group identifies the logical object; all items of a multi-command
	// object share the same group id.
	Group string `json:"group"`
	// Boundary describes where atomic application starts and ends.
	Boundary string `json:"boundary"`
}

// PlanItem is one node of a RestorePlan. Objects that need several Redis
// commands (or several logically ordered steps, like streams) are split into
// multiple items linked by DependsOn and a shared Atomicity group.
type PlanItem struct {
	// ID is stable: the same RDB always yields the same ids, regardless of
	// the order in which events were decoded.
	ID string `json:"id"`
	// Kind is one of select-db, string, list, set, hash, zset,
	// stream-entries, stream-group, stream-pel, function.
	Kind string `json:"kind"`
	// DB is the target logical database.
	DB int `json:"db"`
	// Key is the redis key, empty for select-db and function items.
	Key string `json:"key,omitempty"`
	// Type is the redis type of the source object.
	Type string `json:"type,omitempty"`
	// Encoding is the RDB encoding of the source object.
	Encoding string `json:"encoding,omitempty"`
	// Source points back to the decoded event.
	Source *SourceRef `json:"source,omitempty"`
	// Create holds the commands that create the value.
	Create []Command `json:"create,omitempty"`
	// FollowUp holds commands that must run after Create (expiration,
	// group state, pending entries).
	FollowUp []Command `json:"followUp,omitempty"`
	// Expiration describes the key expiration semantics, if any.
	Expiration *ExpirationSpec `json:"expiration,omitempty"`
	// DependsOn lists ids of items that must be applied before this one.
	DependsOn []string `json:"dependsOn,omitempty"`
	// Atomicity is set for items that are part of a multi-command object.
	Atomicity *AtomicitySpec `json:"atomicity,omitempty"`
	// Notes document lossy or time-sensitive aspects of this item.
	Notes []string `json:"notes,omitempty"`
}

// Issue is a warning or a blocker. Warnings document data that cannot be
// restored losslessly; blockers document objects the plan cannot restore at
// all on the target. Both always carry evidence.
type Issue struct {
	Code     string `json:"code"`
	ItemID   string `json:"itemId,omitempty"`
	DB       int    `json:"db"`
	Key      string `json:"key,omitempty"`
	Evidence string `json:"evidence,omitempty"`
	Message  string `json:"message"`
}

// Stats summarizes plan generation.
type Stats struct {
	Objects        int `json:"objects"`
	Items          int `json:"items"`
	Commands       int `json:"commands"`
	SkippedExpired int `json:"skippedExpired,omitempty"`
	SkippedMeta    int `json:"skippedMeta,omitempty"`
}

// RestorePlan is a deterministic, serializable description of how to
// restore a decoded RDB onto a target Redis. It contains no Go maps, so its
// JSON serialization is byte-stable.
type RestorePlan struct {
	Version         int               `json:"version"`
	ReferenceUnixMs int64             `json:"referenceUnixMs"`
	ExpiredPolicy   ExpiredPolicy     `json:"expiredPolicy"`
	Target          CapabilityProfile `json:"target"`
	Items           []*PlanItem       `json:"items"`
	Warnings        []*Issue          `json:"warnings,omitempty"`
	Blockers        []*Issue          `json:"blockers,omitempty"`
	Stats           *Stats            `json:"stats"`
}

// Lossless reports whether the plan has no blockers.
func (p *RestorePlan) Lossless() bool {
	return len(p.Blockers) == 0
}

// ToJSON serializes the plan deterministically (the plan contains no maps).
func (p *RestorePlan) ToJSON() ([]byte, error) {
	return json.MarshalIndent(p, "", "  ")
}

// findItem returns the item with the given id, or nil.
func (p *RestorePlan) findItem(id string) *PlanItem {
	for _, item := range p.Items {
		if item.ID == id {
			return item
		}
	}
	return nil
}

// objectID builds the stable identity of a decoded object.
func objectID(obj model.RedisObject) string {
	return "obj/" + itoa(obj.GetDBIndex()) + "/" + hexEncode(obj.GetKey()) + "/" + obj.GetType()
}

func hexEncode(s string) string {
	const digits = "0123456789abcdef"
	buf := make([]byte, 0, len(s)*2)
	for i := 0; i < len(s); i++ {
		buf = append(buf, digits[s[i]>>4], digits[s[i]&0xf])
	}
	return string(buf)
}
