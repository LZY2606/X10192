// Package restore builds deterministic, serializable restore plans from
// decoded RDB objects. A RestorePlan describes which commands would be
// needed to rebuild the data set on a target Redis, without connecting to
// or executing against any server, so other programs can safely preview it.
package restore

import (
	"encoding/json"
	"time"
)

// ExpiredPolicy decides how keys whose expiration is already in the past
// (relative to the restore reference time) are handled.
type ExpiredPolicy string

const (
	// ExpiredSkip drops expired keys from the plan and records a warning.
	ExpiredSkip ExpiredPolicy = "skip"
	// ExpiredKeep keeps expired keys with their original absolute
	// expiration and records a warning.
	ExpiredKeep ExpiredPolicy = "keep"
	// ExpiredError makes Build fail if any expired key is encountered.
	ExpiredError ExpiredPolicy = "error"
)

// Item kinds produced by the builder.
const (
	KindSelectDB       = "selectdb"
	KindString         = "string"
	KindList           = "list"
	KindSet            = "set"
	KindHash           = "hash"
	KindZSet           = "zset"
	KindStreamEntries  = "stream-entries"
	KindStreamGroup    = "stream-group"
	KindStreamConsumer = "stream-consumer"
	KindStreamPEL      = "stream-pel"
	KindFunction       = "function"
	KindModule         = "module"
	KindUnsupported    = "unsupported"
)

// Command roles, distinguishing create commands from follow-up commands.
const (
	// RoleCreate is the command that creates the key/value.
	RoleCreate = "create"
	// RoleExpire sets key or field level expiration.
	RoleExpire = "expire"
	// RoleState restores auxiliary state (groups, consumers, PEL).
	RoleState = "state"
)

// Warning codes.
const (
	WarnExpiredKeySkipped   = "expired-key-skipped"
	WarnExpiredKeyKept      = "expired-key-kept"
	WarnStreamDeletedEntry  = "stream-deleted-entries"
	WarnStreamMetadataLoss  = "stream-metadata-loss"
	WarnEntriesReadLoss     = "entries-read-loss"
	WarnConsumerTimeLoss    = "consumer-seen-time-loss"
	WarnConsumerUnsupported = "createconsumer-unsupported"
	WarnPELIdleReset        = "pel-idle-reset"
)

// Blocker codes.
const (
	BlockCapability          = "capability-blocker"
	BlockUnknownModuleType   = "unknown-module-type"
	BlockModuleValue         = "module-value-unsupported"
	BlockUnsupportedEncoding = "unsupported-encoding"
)

// Options configures plan generation.
type Options struct {
	// ReferenceTime is the restore reference time. Relative TTLs in the
	// plan are computed against it and expiration replayability is defined
	// with respect to it. Required.
	ReferenceTime time.Time
	// ExpiredPolicy decides the fate of already-expired keys. Required.
	ExpiredPolicy ExpiredPolicy
	// Target describes the capability profile of the restore target.
	// The zero value is replaced by DefaultProfile().
	Target CapabilityProfile
}

func (o *Options) normalize() {
	if o.Target == (CapabilityProfile{}) {
		o.Target = DefaultProfile()
	}
}

// Command is a single Redis command in a plan item.
type Command struct {
	// Name is the command name, e.g. SET, XADD, PEXPIREAT.
	Name string `json:"name"`
	// Args are the command arguments including the key.
	Args []string `json:"args"`
	// Role is one of RoleCreate, RoleExpire, RoleState.
	Role string `json:"role"`
}

// ExpirationPlan describes key expiration semantics. Both the absolute
// timestamp and the TTL relative to the restore reference time are recorded
// so the plan can be replayed under that reference time.
type ExpirationPlan struct {
	// AbsoluteMs is the absolute expiration time in unix milliseconds.
	AbsoluteMs int64 `json:"absoluteMs"`
	// RelativeMs is AbsoluteMs minus the restore reference time in ms.
	// It is negative when the key is already expired at reference time.
	RelativeMs int64 `json:"relativeMs"`
	// Expired reports whether the key is expired at the reference time.
	Expired bool `json:"expired"`
}

// Source locates the origin of a plan item in the input.
type Source struct {
	// Offset is the byte offset of the object in the RDB input,
	// or -1 when unknown. It is supplied by the caller and travels with
	// the object, so it is stable regardless of processing order.
	Offset int64 `json:"offset"`
}

// StreamGroupDetail carries stream group state that cannot be fully
// expressed as command arguments.
type StreamGroupDetail struct {
	// Group is the consumer group name.
	Group string `json:"group"`
	// LastDeliveredID is the group last-delivered-id.
	LastDeliveredID string `json:"lastDeliveredId"`
	// EntriesRead is the group entries-read counter (0 when unknown).
	EntriesRead uint64 `json:"entriesRead"`
	// Consumer is set on stream-consumer items.
	Consumer string `json:"consumer,omitempty"`
	// PELOwner is set on stream-pel commands: the consumer that owns the
	// pending entries being restored.
	PELOwner string `json:"pelOwner,omitempty"`
}

// PlanItem is a single deterministic step of a RestorePlan.
type PlanItem struct {
	// ID is a stable, content-derived identifier, independent of input
	// ordering and Go map iteration order.
	ID string `json:"id"`
	// Kind is one of the Kind* constants.
	Kind string `json:"kind"`
	// DB is the target logical database.
	DB int `json:"db"`
	// Key is the redis key, empty for selectdb and function items.
	Key string `json:"key,omitempty"`
	// Group identifies the logical group this item belongs to. All items
	// and commands restoring one logical object share a group id.
	Group string `json:"group,omitempty"`
	// Atomicity describes the atomicity boundary of the logical group.
	Atomicity string `json:"atomicity,omitempty"`
	// Commands are the create commands followed by follow-up commands.
	Commands []Command `json:"commands,omitempty"`
	// Expiration holds expiration semantics, nil for persistent keys.
	Expiration *ExpirationPlan `json:"expiration,omitempty"`
	// DependsOn lists ids of items that must be applied before this one.
	DependsOn []string `json:"dependsOn,omitempty"`
	// Stream carries stream group/consumer/PEL detail.
	Stream *StreamGroupDetail `json:"stream,omitempty"`
	// Source records the origin of the item.
	Source Source `json:"source"`
	// Blocked is true when the item cannot be restored losslessly; see
	// the plan Blockers for evidence.
	Blocked bool `json:"blocked,omitempty"`
}

// Warning describes a lossy or noteworthy aspect of the plan.
type Warning struct {
	ItemID  string `json:"itemId"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Blocker describes an item that cannot be restored losslessly, with
// evidence. Blocked items are never silently skipped: they appear in the
// plan with Blocked set and no (or partial) commands.
type Blocker struct {
	ItemID string `json:"itemId"`
	Code   string `json:"code"`
	// Feature is the missing capability, when capability related.
	Feature string `json:"feature,omitempty"`
	// Required is the minimum redis version supporting Feature.
	Required string `json:"required,omitempty"`
	// Target is the configured target profile version.
	Target string `json:"target,omitempty"`
	Message  string `json:"message"`
}

// MetadataEntry is a deterministic key-value pair for aux/resizedb data.
type MetadataEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RestorePlan is the deterministic, serializable output of the builder.
// It contains no Go maps, so its JSON encoding is byte-stable.
type RestorePlan struct {
	// Version is the plan format version.
	Version int `json:"version"`
	// ReferenceTime is the restore reference time in UTC RFC3339Nano.
	ReferenceTime string `json:"referenceTime"`
	// ReferenceTimeMs is the restore reference time in unix milliseconds.
	ReferenceTimeMs int64 `json:"referenceTimeMs"`
	// ExpiredPolicy is the policy applied to expired keys.
	ExpiredPolicy ExpiredPolicy `json:"expiredPolicy"`
	// Target is the capability profile used.
	Target CapabilityProfile `json:"target"`
	// Items are the plan steps, deterministically ordered.
	Items []*PlanItem `json:"items"`
	// Warnings lists lossy aspects, deterministically ordered.
	Warnings []Warning `json:"warnings"`
	// Blockers lists items that cannot be restored losslessly.
	Blockers []Blocker `json:"blockers"`
	// Metadata carries aux and resize-hint information.
	Metadata []MetadataEntry `json:"metadata,omitempty"`
}

// Bytes returns the deterministic JSON encoding of the plan.
func (p *RestorePlan) Bytes() ([]byte, error) {
	return json.Marshal(p)
}
