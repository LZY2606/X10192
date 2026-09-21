// Package restoreplan turns decoded RDB objects into a deterministic,
// serializable RestorePlan. The plan describes which commands would be
// needed to restore the data into a target Redis, without ever connecting
// to or executing against a real server. It is meant to be previewed,
// diffed and audited by other programs.
package restoreplan

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ExpiredPolicy decides how keys that are already expired at the restore
// reference time are handled.
type ExpiredPolicy string

const (
	// ExpiredSkip drops keys that are already expired at RefTime. A
	// plan-level warning records every dropped key.
	ExpiredSkip ExpiredPolicy = "skip"
	// ExpiredKeep restores expired keys anyway, keeping their absolute
	// expiration. A warning is attached to the item because the key may
	// expire immediately after restore.
	ExpiredKeep ExpiredPolicy = "keep"
	// ExpiredError turns every expired key into a blocker.
	ExpiredError ExpiredPolicy = "error"
)

// Item kinds produced by the planner.
const (
	KindSelectDB       = "select-db"
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
)

// kindRank orders items that belong to the same key so that stream entries
// always precede groups, and groups precede consumer/PEL state.
var kindRank = map[string]int{
	KindString:         0,
	KindList:           0,
	KindSet:            0,
	KindHash:           0,
	KindZSet:           0,
	KindFunction:       0,
	KindStreamEntries:  0,
	KindStreamGroup:    1,
	KindStreamConsumer: 2,
	KindStreamPEL:      3,
}

// CapabilityProfile describes what the target Redis server supports. The
// planner uses it to pick alternative commands or to raise an evidenced
// blocker when a required feature is missing.
type CapabilityProfile struct {
	Name string `json:"name"`
	// Version is the target server version, e.g. "7.2".
	Version string `json:"version"`
	// Functions reports FUNCTION LOAD/RESTORE support (Redis >= 7.0).
	Functions bool `json:"functions"`
	// HashFieldExpire reports HEXPIRE/HPEXPIREAT support (Redis >= 7.4).
	HashFieldExpire bool `json:"hashFieldExpire"`
	// XGroupCreateConsumer reports XGROUP CREATECONSUMER support (Redis >= 6.2).
	XGroupCreateConsumer bool `json:"xgroupCreateConsumer"`
	// XSetIDMeta reports XSETID ... ENTRIESADDED/MAXDELETEDID support (Redis >= 7.0).
	XSetIDMeta bool `json:"xsetidMeta"`
	// XGroupEntriesRead reports XGROUP CREATE ... ENTRIESREAD support (Redis >= 7.0).
	XGroupEntriesRead bool `json:"xgroupEntriesRead"`
}

// ProfileForVersion derives a CapabilityProfile from a version string such
// as "6.2.14" or "7.4".
func ProfileForVersion(name, version string) CapabilityProfile {
	atLeast := func(major, minor int) bool {
		return versionAtLeast(version, major, minor)
	}
	return CapabilityProfile{
		Name:                 name,
		Version:              version,
		Functions:            atLeast(7, 0),
		HashFieldExpire:      atLeast(7, 4),
		XGroupCreateConsumer: atLeast(6, 2),
		XSetIDMeta:           atLeast(7, 0),
		XGroupEntriesRead:    atLeast(7, 0),
	}
}

// DefaultProfile targets a stock Redis 7.2 server.
var DefaultProfile = ProfileForVersion("redis", "7.2")

func versionAtLeast(version string, major, minor int) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	maj, err1 := strconv.Atoi(parts[0])
	min, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	if maj != major {
		return maj > major
	}
	return min >= minor
}

// Options controls plan generation.
type Options struct {
	// RefTime is the restore reference time. Absolute expirations are
	// replayed as PEXPIREAT/HPEXPIREAT at this instant, and relative TTLs
	// in the plan are computed against it. Zero value uses time.Now().
	RefTime time.Time
	// ExpiredPolicy decides the fate of keys already expired at RefTime.
	// Defaults to ExpiredSkip.
	ExpiredPolicy ExpiredPolicy
	// Profile describes the target server capabilities. Defaults to
	// DefaultProfile.
	Profile CapabilityProfile
	// ChunkSize is the maximum number of elements carried by a single
	// write command (HSET/RPUSH/SADD/ZADD). Objects larger than this are
	// split into several commands inside one logical group. Defaults to 128.
	ChunkSize int
}

func (o *Options) normalize() {
	if o.RefTime.IsZero() {
		o.RefTime = time.Now()
	}
	if o.ExpiredPolicy == "" {
		o.ExpiredPolicy = ExpiredSkip
	}
	if o.Profile.Name == "" && o.Profile.Version == "" {
		o.Profile = DefaultProfile
	}
	if o.ChunkSize <= 0 {
		o.ChunkSize = 128
	}
}

// Command is one Redis command in the plan. Args are ordered and contain
// no map-derived data, so serialization is deterministic.
type Command struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
	// Note documents version requirements or replay semantics.
	Note string `json:"note,omitempty"`
}

// Expiry describes the expiration semantics of an item. AbsUnixMs is the
// absolute expiration replayed via PEXPIREAT; TTLMs is the same instant
// expressed relative to the plan reference time.
type Expiry struct {
	AbsUnixMs int64 `json:"absUnixMs"`
	TTLMs     int64 `json:"ttlMs"`
	// Expired is true when the key is already expired at RefTime.
	Expired bool `json:"expired"`
	// Command is the command used to replay the expiration ("PEXPIREAT").
	Command string `json:"command"`
}

// Source locates the decoded object an item was derived from. It is a
// logical position (db/key/type/part), independent of the order in which
// objects were delivered by the decoder.
type Source struct {
	DB       int    `json:"db"`
	Key      string `json:"key,omitempty"`
	Type     string `json:"type"`
	Encoding string `json:"encoding,omitempty"`
	// Part distinguishes items derived from the same object, e.g.
	// "entries", "group:<name>", "consumer:<group>/<name>" or "pel:<group>".
	Part string `json:"part,omitempty"`
}

// PlanItem is a single step of the restore plan.
type PlanItem struct {
	// ID is a stable identifier assigned after deterministic sorting.
	ID   string `json:"id"`
	Kind string `json:"kind"`
	DB   int    `json:"db"`
	Key  string `json:"key,omitempty"`
	// Group links items that belong to the same logical object.
	Group string `json:"group,omitempty"`
	// Atomicity documents the atomicity boundary of multi-command items.
	Atomicity string `json:"atomicity,omitempty"`
	// DependsOn lists item IDs that must be applied before this item.
	DependsOn []string `json:"dependsOn,omitempty"`
	// CreateCommands build the value itself.
	CreateCommands []Command `json:"createCommands,omitempty"`
	// FollowUpCommands apply state on top of the value (expiry, XSETID,
	// XCLAIM, ...).
	FollowUpCommands []Command `json:"followUpCommands,omitempty"`
	Expiry           *Expiry   `json:"expiry,omitempty"`
	Warnings         []string  `json:"warnings,omitempty"`
	Source           Source    `json:"source"`

	// depKey and depends are internal, resolved to ID/DependsOn after sorting.
	depKey  string
	depends []string
}

// Warning is a non-fatal, plan-level notice.
type Warning struct {
	ItemID  string `json:"itemId,omitempty"`
	Message string `json:"message"`
}

// Blocker marks data that cannot be losslessly restored with the given
// capability profile. Blockers always carry evidence.
type Blocker struct {
	ItemID   string `json:"itemId,omitempty"`
	Reason   string `json:"reason"`
	Evidence string `json:"evidence"`
}

// Stats summarizes plan generation.
type Stats struct {
	Objects  int `json:"objects"`
	Items    int `json:"items"`
	Skipped  int `json:"skipped"`
	Warnings int `json:"warnings"`
	Blockers int `json:"blockers"`
}

// RestorePlan is the deterministic, serializable output of the planner.
// It contains no Go maps, so JSON serialization is byte-stable.
type RestorePlan struct {
	// Version is the plan schema version.
	Version int `json:"version"`
	// RefTime is the restore reference time in UTC, RFC3339Nano.
	RefTime       string            `json:"refTime"`
	ExpiredPolicy ExpiredPolicy     `json:"expiredPolicy"`
	Profile       CapabilityProfile `json:"profile"`
	Items         []*PlanItem       `json:"items"`
	Warnings      []Warning         `json:"warnings,omitempty"`
	Blockers      []Blocker         `json:"blockers,omitempty"`
	Stats         Stats             `json:"stats"`
}

const atomicityNote = "object is restored by multiple commands/items; not atomic unless wrapped in MULTI/EXEC or a Lua script"

func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'g', -1, 64)
}

func streamIDString(ms, seq uint64) string {
	return fmt.Sprintf("%d-%d", ms, seq)
}
