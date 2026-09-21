// Package restore turns decoded RDB objects into a deterministic, serializable
// RestorePlan that can be previewed without connecting to Redis.
//
// The plan never executes commands. It only describes:
//
//   - which logical databases must be selected,
//   - which commands create each object and which commands follow,
//   - how expiration times are replayed relative to a chosen reference time,
//   - how the generated nodes depend on each other,
//   - which information cannot be restored losslessly (warnings/blockers).
//
// All slice contents are sorted by content, never by Go map iteration order,
// so the same RDB always yields a byte-identical plan.
package restore

import (
	"encoding/json"
	"time"
)

// RestorePlanVersion is the schema version of RestorePlan.
const RestorePlanVersion = 1

// ExpiredPolicy decides what happens to a key whose RDB expiration time is
// already in the past relative to the restore reference time.
type ExpiredPolicy string

const (
	// ExpiredPolicySkip omits expired keys from the plan (they are listed as
	// skipped items with a warning).
	ExpiredPolicySkip ExpiredPolicy = "skip"
	// ExpiredPolicyKeep restores expired keys as persistent keys, dropping
	// their TTL. A warning records the dropped expiration.
	ExpiredPolicyKeep ExpiredPolicy = "keep"
	// ExpiredPolicyError marks expired keys as blocking items and makes the
	// builder return an error. The plan is still produced for preview, but it
	// is marked non-executable.
	ExpiredPolicyError ExpiredPolicy = "error"
)

// ItemStatus describes whether a plan item is replayable.
type ItemStatus string

const (
	// ItemStatusActive means all nodes of the item can be replayed.
	ItemStatusActive ItemStatus = "active"
	// ItemStatusSkipped means the item was intentionally not replayed
	// (for example an expired key under ExpiredPolicySkip).
	ItemStatusSkipped ItemStatus = "skipped"
	// ItemStatusBlocked means the target cannot restore the item without data
	// loss or missing capabilities. Nodes are still emitted for inspection.
	ItemStatusBlocked ItemStatus = "blocked"
)

// WarningSeverity distinguishes recoverable losses from hard blockers.
type WarningSeverity string

const (
	// SeverityWarning marks a lossy-but-replayable condition.
	SeverityWarning WarningSeverity = "warning"
	// SeverityError blocks replay on the chosen target profile.
	SeverityError WarningSeverity = "error"
)

// Stable warning/blocker codes. Codes are part of the public API and may be
// matched by programs consuming RestorePlan.
const (
	// WarnKeyExpiredSkipped: key already expired at reference time and was skipped.
	WarnKeyExpiredSkipped = "key-expired-skipped"
	// WarnKeyExpiredKept: key already expired but restored without TTL.
	WarnKeyExpiredKept = "key-expired-ttl-dropped"
	// WarnKeyExpiredBlocked: expired key under ExpiredPolicyError.
	WarnKeyExpiredBlocked = "key-expired-blocked"
	// WarnPEXPIREATUnsupported: target server is older than PEXPIREAT (Redis 2.6).
	WarnPEXPIREATUnsupported = "pexpireat-unsupported"
	// WarnHashFieldExpiryUnsupported: HPEXPIREAT requires Redis 7.4+.
	WarnHashFieldExpiryUnsupported = "hash-field-expiry-unsupported"
	// WarnStreamDeletedEntries: deleted stream messages cannot be replayed.
	WarnStreamDeletedEntries = "stream-deleted-entries"
	// WarnStreamMetaLoss: last-generated-id / added-entries counters cannot be set.
	WarnStreamMetaLoss = "stream-metadata-not-replayable"
	// WarnStreamEmptyWorkaround: empty stream reconstructed with a delete trick.
	WarnStreamEmptyWorkaround = "stream-empty-workaround"
	// WarnEntriesReadUnsupported: XGROUP ENTRIESREAD requires Redis 7.0.
	WarnEntriesReadUnsupported = "group-entriesread-unsupported"
	// WarnConsumerNotCreated: XGROUP CREATECONSUMER requires Redis 7.0.
	WarnConsumerNotCreated = "consumer-create-unsupported"
	// WarnConsumerTimeLoss: consumer seen/active timestamps are not replayable.
	WarnConsumerTimeLoss = "consumer-time-not-replayable"
	// WarnXClaimTimeUnsupported: XCLAIM TIME requires Redis 6.2, IDLE is lossy.
	WarnXClaimTimeUnsupported = "xclaim-time-unsupported"
	// WarnPELOwnerMissing: pending entry is not claimed by any consumer.
	WarnPELOwnerMissing = "pel-owner-missing"
	// WarnPELEntryMissing: pending entry id has no matching stream message.
	WarnPELEntryMissing = "pel-entry-missing"
	// WarnFunctionsUnsupported: FUNCTION LOAD requires Redis 7.0.
	WarnFunctionsUnsupported = "functions-unsupported"
	// WarnFunctionsPayloadEmpty: function library payload was empty or unreadable.
	WarnFunctionsPayloadEmpty = "functions-payload-empty"
	// WarnModuleUnsupported: module value cannot be reproduced without the module.
	WarnModuleUnsupported = "module-value-unsupported"
	// WarnUnknownObject: object type/encoding is not understood by this library.
	WarnUnknownObject = "unknown-object-unsupported"
)

// RestorePlan is the deterministic, JSON-serializable result of planning a
// restore. Its output never depends on Go map iteration order.
type RestorePlan struct {
	// Version is RestorePlanVersion.
	Version int `json:"version"`
	// ReferenceTime is the wall-clock instant used to turn absolute RDB
	// expiration times into relative TTLs. It is always explicit.
	ReferenceTime time.Time `json:"referenceTime"`
	// ExpiredPolicy is the policy applied to already-expired keys.
	ExpiredPolicy ExpiredPolicy `json:"expiredPolicy"`
	// Target describes the Redis server the plan was generated for.
	Target *CapabilityProfile `json:"target"`
	// Items lists every logical restore item in stable execution order.
	Items []*PlanItem `json:"items"`
	// Databases lists the databases touched by the plan, sorted by index.
	// Switching databases must be replayed explicitly.
	Databases []*DatabasePlan `json:"databases"`
	// Warnings lists lossless/loss/blocker findings in stable order.
	Warnings []*Warning `json:"warnings"`
	// Stats summarizes the plan.
	Stats PlanStats `json:"stats"`
	// Executable is false when at least one item is blocked.
	Executable bool `json:"executable"`
}

// DatabasePlan describes a logical Redis database that must be selected.
type DatabasePlan struct {
	// DB is the zero-based database index.
	DB int `json:"db"`
	// SwitchCommand is the explicit SELECT command for this database.
	SwitchCommand *Command `json:"switchCommand"`
	// ItemIDs lists the item ids belonging to this database in execution order.
	ItemIDs []string `json:"itemIds"`
}

// PlanStats summarizes a RestorePlan.
type PlanStats struct {
	ItemCount      int `json:"itemCount"`
	ActiveItems    int `json:"activeItems"`
	SkippedItems   int `json:"skippedItems"`
	BlockedItems   int `json:"blockedItems"`
	NodeCount      int `json:"nodeCount"`
	CommandCount   int `json:"commandCount"`
	WarningCount   int `json:"warningCount"`
	BlockerCount   int `json:"blockerCount"`
	TouchedDBCount int `json:"touchedDBCount"`
}

// PlanItem is one logical object to restore (a key, or the global function
// libraries). An item may expand to several nodes and commands; even when it
// expands to many commands it keeps one stable id and one atomicity story.
type PlanItem struct {
	// ID is the stable identifier, e.g. "db0:key:mykey" or "functions".
	ID string `json:"id"`
	// Kind is "key" or "functions".
	Kind string `json:"kind"`
	// DB is the target database index. -1 for global items (functions).
	DB int `json:"db"`
	// Key is the Redis key for key items, empty otherwise.
	Key string `json:"key,omitempty"`
	// Type is the model type (string/list/set/hash/zset/stream/module/...).
	Type string `json:"type,omitempty"`
	// Encoding is the exact RDB encoding the object was stored with.
	Encoding string `json:"encoding,omitempty"`
	// Source locates the object inside the decoded RDB input.
	Source Source `json:"source"`
	// Status is active / skipped / blocked.
	Status ItemStatus `json:"status"`
	// DependsOn lists item ids that must be fully applied before this item.
	// Functions are applied before every key item; otherwise items within a
	// database are independent.
	DependsOn []string `json:"dependsOn"`
	// Nodes are the replayable nodes in dependency order.
	Nodes []*PlanNode `json:"nodes"`
	// Expiration describes key TTL semantics, nil for persistent/global items.
	Expiration *ExpirationSpec `json:"expiration,omitempty"`
	// WarningIDs references findings that concern this whole item.
	WarningIDs []string `json:"warningIds,omitempty"`
}

// MarshalJSON emits slice fields as [] rather than null for stable output.
func (it *PlanItem) MarshalJSON() ([]byte, error) {
	type alias PlanItem
	a := alias(*it)
	if a.DependsOn == nil {
		a.DependsOn = []string{}
	}
	if a.Nodes == nil {
		a.Nodes = []*PlanNode{}
	}
	if a.WarningIDs == nil {
		a.WarningIDs = []string{}
	}
	return json.Marshal(a)
}

// Source locates an object in the decoded RDB.
type Source struct {
	// DB is the RDB database index.
	DB int `json:"db"`
	// Key is the key as found in the RDB.
	Key string `json:"key,omitempty"`
	// Offset is an optional byte offset provided by the caller via an Event;
	// 0 means unknown. It never affects plan ordering or ids.
	Offset int64 `json:"offset,omitempty"`
	// Sequence is an optional caller-provided ordinal; 0 means unknown.
	Sequence int64 `json:"sequence,omitempty"`
}

// PlanNode is a replay step within an item (create, group, consumer, pel, ...).
// Nodes form a DAG inside the item via DependsOn.
type PlanNode struct {
	// ID is the stable node identifier, unique inside the plan.
	ID string `json:"id"`
	// Kind describes the node role: "create", "expire", "group",
	// "consumer", "pel", "functions", "empty-stream-workaround".
	Kind string `json:"kind"`
	// Label is a short human-readable label (stable, not for parsing).
	Label string `json:"label"`
	// DependsOn lists node ids (within the same item) applied first.
	DependsOn []string `json:"dependsOn"`
	// Groups are the command groups executed for this node, in order.
	Groups []*CommandGroup `json:"groups"`
	// Notes carries replay semantics (PEL owner/idle handling, loss details).
	Notes []string `json:"notes,omitempty"`
	// WarningIDs references findings attached to this node.
	WarningIDs []string `json:"warningIds,omitempty"`
}

// MarshalJSON emits slice fields as [] rather than null.
func (n *PlanNode) MarshalJSON() ([]byte, error) {
	type alias PlanNode
	a := alias(*n)
	if a.DependsOn == nil {
		a.DependsOn = []string{}
	}
	if a.Groups == nil {
		a.Groups = []*CommandGroup{}
	}
	if a.Notes == nil {
		a.Notes = []string{}
	}
	if a.WarningIDs == nil {
		a.WarningIDs = []string{}
	}
	return json.Marshal(a)
}

// CommandGroup groups one or more commands with a declared atomicity boundary.
type CommandGroup struct {
	// Commands are executed in listed order.
	Commands []*Command `json:"commands"`
	// Atomicity is "single" (one command), "multi-exec" (wrapped in
	// MULTI/EXEC), or "non-atomic" (a batch with no failure atomicity).
	Atomicity string `json:"atomicity"`
	// Purpose explains why the group exists ("create", "field-expire", ...).
	Purpose string `json:"purpose"`
}

// MarshalJSON guarantees non-null command slices.
func (g *CommandGroup) MarshalJSON() ([]byte, error) {
	type alias CommandGroup
	a := alias(*g)
	if a.Commands == nil {
		a.Commands = []*Command{}
	}
	return json.Marshal(a)
}

const (
	AtomicitySingle    = "single"
	AtomicityMultiExec = "multi-exec"
	AtomicityNonAtomic = "non-atomic"
)

// Command is a serializable Redis command with verbatim byte-string args.
type Command struct {
	// Args holds the command and its arguments exactly as they must be sent.
	// Strings are Go strings carrying arbitrary bytes (binary-safe); see
	// MarshalJSON/UnmarshalJSON for safe JSON transport.
	Args []string `json:"args"`
}

// ExpirationSpec records both absolute and relative TTL semantics so the plan
// is replayable at, but not tied to, the chosen reference time.
type ExpirationSpec struct {
	// AbsoluteAtMS is the exact expiration instant from the RDB (unix ms).
	AbsoluteAtMS int64 `json:"absoluteAtMs"`
	// RelativeTTLMS is max(0, AbsoluteAtMS-referenceTime): the remaining TTL
	// at the reference time. Use PEXPIRE with this value for relative replay.
	RelativeTTLMS int64 `json:"relativeTtlMs"`
	// Precision is "second" or "millisecond", reflecting the original RDB
	// opcode (EXPIRE sec vs PEXPIRE ms).
	Precision string `json:"precision"`
	// Expired is true when AbsoluteAtMS is not after the reference time.
	Expired bool `json:"expired"`
	// AbsoluteCommand reproduces the original absolute deadline.
	AbsoluteCommand *Command `json:"absoluteCommand,omitempty"`
	// RelativeCommand sets the remaining TTL measured from reference time.
	RelativeCommand *Command `json:"relativeCommand,omitempty"`
}

// Warning is a loss or blocker finding. Multiple objects sharing a finding are
// aggregated into one warning with a sorted Evidence list.
type Warning struct {
	// ID is stable and derived from the code plus affected item ids.
	ID string `json:"id"`
	// Code is one of the Warn* constants.
	Code string `json:"code"`
	// Severity is warning or error.
	Severity WarningSeverity `json:"severity"`
	// Message is a human-readable explanation.
	Message string `json:"message"`
	// Evidence lists stable affected item or node ids, sorted.
	Evidence []string `json:"evidence"`
}

// commandJSON / rawCommand keeps binary-safe args through JSON.
type rawCommand struct {
	Args []string `json:"args"`
}

// MarshalJSON guarantees args is emitted as [] even when empty and keeps
// arbitrary bytes intact (JSON strings are byte-transparent).
func (c *Command) MarshalJSON() ([]byte, error) {
	args := c.Args
	if args == nil {
		args = []string{}
	}
	return json.Marshal(rawCommand{Args: args})
}

// UnmarshalJSON implements the inverse of MarshalJSON.
func (c *Command) UnmarshalJSON(data []byte) error {
	var raw rawCommand
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.Args = raw.Args
	return nil
}
