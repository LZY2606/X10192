// Package restore builds deterministic, serializable restore plans from
// decoded RDB objects.
//
// A RestorePlan describes, for every decodable object, which Redis commands
// would recreate it on a target server: the target logical DB, the create
// command, any follow-up commands (expiration, stream groups, consumer and
// pending-entry-list state), the dependencies between those commands and the
// data-loss warnings for anything that cannot be restored losslessly.
//
// The package never connects to a Redis server and never executes the
// commands it produces, so plans are safe to generate, inspect and diff in
// any program. Plan output is fully deterministic: items are ordered by a
// canonical sort of the input objects and every Go map is traversed in
// sorted key order, therefore the same RDB always yields a byte-identical
// plan regardless of map iteration order or the order of the input events.
package restore

import (
	"encoding/base64"
	"encoding/json"
	"time"
	"unicode/utf8"
)

// FormatVersion is the version of the RestorePlan serialization format.
const FormatVersion = 1

// ExpiredPolicy decides how keys that are already expired at the plan
// reference time are handled.
type ExpiredPolicy string

const (
	// ExpiredPolicySkip drops expired keys from the plan and records a warning.
	ExpiredPolicySkip ExpiredPolicy = "skip"
	// ExpiredPolicyKeep restores expired keys without their expiration and
	// records a warning.
	ExpiredPolicyKeep ExpiredPolicy = "keep"
	// ExpiredPolicyError turns every expired key into a blocker.
	ExpiredPolicyError ExpiredPolicy = "error"
)

// Role classifies what a plan item does.
type Role string

const (
	// RoleSelectDB is an explicit logical DB switch (SELECT).
	RoleSelectDB Role = "select-db"
	// RoleCreate is the command that creates the key (SET/RPUSH/SADD/HMSET/ZADD).
	RoleCreate Role = "create"
	// RoleExpire sets the key expiration (PEXPIREAT).
	RoleExpire Role = "expire"
	// RoleFieldExpire sets hash field expirations (HPEXPIREAT).
	RoleFieldExpire Role = "field-expire"
	// RoleStreamEntry appends one stream entry (XADD).
	RoleStreamEntry Role = "stream-entry"
	// RoleStreamMeta restores stream metadata (XSETID).
	RoleStreamMeta Role = "stream-meta"
	// RoleStreamGroup creates a consumer group (XGROUP CREATE).
	RoleStreamGroup Role = "stream-group"
	// RoleStreamConsumer creates a group consumer (XGROUP CREATECONSUMER).
	RoleStreamConsumer Role = "stream-consumer"
	// RoleStreamPEL restores a pending entry of a group (XCLAIM ... FORCE).
	RoleStreamPEL Role = "stream-pel"
	// RoleFunction loads a function library (FUNCTION LOAD).
	RoleFunction Role = "function"
)

// Arg is a single command argument. It serializes deterministically: printable
// UTF-8 is emitted as a plain JSON string, anything else as "base64:<std-b64>".
type Arg []byte

// MarshalJSON implements json.Marshaler.
func (a Arg) MarshalJSON() ([]byte, error) {
	if isPrintable(a) {
		return json.Marshal(string(a))
	}
	return json.Marshal("base64:" + base64.StdEncoding.EncodeToString(a))
}

func isPrintable(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// Command is one Redis command. It is never executed by this package.
type Command struct {
	Name string `json:"name"`
	Args []Arg  `json:"args,omitempty"`
}

// Cmd builds a Command from a name and plain string/byte-slice arguments.
func Cmd(name string, args ...[]byte) Command {
	c := Command{Name: name}
	for _, a := range args {
		c.Args = append(c.Args, Arg(a))
	}
	return c
}

// Source locates the object a plan item was derived from. Ordinal is the
// position of the item in the plan's canonical ordering; Locator is a
// human-readable "db<N>:key=<k>:type=<t>" description. Both are stable across
// runs for the same RDB content.
type Source struct {
	Ordinal int    `json:"ordinal"`
	Locator string `json:"locator"`
}

// ExpirationPlan describes how a key expiration is replayed. The absolute
// timestamp is replayed with PEXPIREAT and is therefore correct no matter
// when the plan is executed; TTLMsAtReference expresses the same expiration
// relative to the plan reference time for executors that prefer PEXPIRE.
type ExpirationPlan struct {
	AbsoluteUnixMs     int64  `json:"absoluteUnixMs"`
	TTLMsAtReference   int64  `json:"ttlMsAtReference"`
	ExpiredAtReference bool   `json:"expiredAtReference,omitempty"`
	Semantics          string `json:"semantics"`
}

// FieldExpiration describes one hash field expiration (Redis 7.4 HFE).
type FieldExpiration struct {
	Field              string `json:"field"`
	AbsoluteUnixMs     int64  `json:"absoluteUnixMs"`
	TTLMsAtReference   int64  `json:"ttlMsAtReference"`
	ExpiredAtReference bool   `json:"expiredAtReference,omitempty"`
}

// PlanItem is one step of the restore plan. Items that belong to the same
// logical object share a GroupID.
type PlanItem struct {
	ID         string          `json:"id"`
	GroupID    string          `json:"groupId,omitempty"`
	Role       Role            `json:"role"`
	DB         int             `json:"db"`
	Key        string          `json:"key,omitempty"`
	Type       string          `json:"type,omitempty"`
	Source     Source          `json:"source"`
	Commands   []Command       `json:"commands,omitempty"`
	DependsOn  []string        `json:"dependsOn,omitempty"`
	Expiration *ExpirationPlan `json:"expiration,omitempty"`
	// FieldExpirations is set on RoleFieldExpire items, sorted by field.
	FieldExpirations []FieldExpiration `json:"fieldExpirations,omitempty"`
	// Notes carry machine-readable caveats about this item, e.g. stream
	// last-delivered-id, entries-read or PEL idle-time semantics.
	Notes []string `json:"notes,omitempty"`
}

// Group is the logical unit of one decoded RDB object. Objects that need
// several Redis commands keep all of them in one group; Atomicity documents
// the atomicity boundary of executing the group.
type Group struct {
	ID        string   `json:"id"`
	DB        int      `json:"db"`
	Key       string   `json:"key"`
	Type      string   `json:"type"`
	Encoding  string   `json:"encoding,omitempty"`
	ItemIDs   []string `json:"itemIds"`
	Atomic    bool     `json:"atomic"`
	Atomicity string   `json:"atomicity"`
}

// Warning describes a lossy or approximate restore step. Warnings never
// block execution but the listed data cannot be restored losslessly.
type Warning struct {
	Code    string `json:"code"`
	GroupID string `json:"groupId,omitempty"`
	Key     string `json:"key,omitempty"`
	Message string `json:"message"`
}

// Blocker describes data that cannot be restored at all with the selected
// capability profile. A plan with blockers is incomplete by construction.
type Blocker struct {
	Code     string `json:"code"`
	GroupID  string `json:"groupId,omitempty"`
	Key      string `json:"key,omitempty"`
	Message  string `json:"message"`
	Evidence string `json:"evidence"`
}

// Stats summarizes the plan.
type Stats struct {
	Objects         int `json:"objects"`
	Groups          int `json:"groups"`
	Items           int `json:"items"`
	Commands        int `json:"commands"`
	ExpiredSkipped  int `json:"expiredSkipped,omitempty"`
	MetadataSkipped int `json:"metadataSkipped,omitempty"`
	Warnings        int `json:"warnings"`
	Blockers        int `json:"blockers"`
}

// RestorePlan is the deterministic, serializable output of this package.
type RestorePlan struct {
	Version       int           `json:"version"`
	ReferenceTime time.Time     `json:"referenceTime"`
	ExpiredPolicy ExpiredPolicy `json:"expiredPolicy"`
	Profile       string        `json:"profile"`
	DBs           []int         `json:"dbs"`
	Groups        []*Group      `json:"groups"`
	Items         []*PlanItem   `json:"items"`
	Warnings      []*Warning    `json:"warnings,omitempty"`
	Blockers      []*Blocker    `json:"blockers,omitempty"`
	Stats         Stats         `json:"stats"`
}

// Bytes returns the canonical JSON serialization of the plan. It is
// byte-identical for identical inputs.
func (p *RestorePlan) Bytes() ([]byte, error) {
	return json.MarshalIndent(p, "", "  ")
}

// Executable reports whether the plan has no blockers.
func (p *RestorePlan) Executable() bool {
	return len(p.Blockers) == 0
}
