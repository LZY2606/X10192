// Package restoreplan turns decoded RDB events/objects (model.RedisObject)
// into a deterministic, serializable RestorePlan.
//
// The planner never connects to Redis and never executes commands: it only
// describes what a restore would look like. Callers can preview, persist or
// diff a plan before applying it.
package restoreplan

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// ExpiredPolicy decides what happens when a key (or hash field) expiration is
// already in the past relative to Options.ReferenceTime.
type ExpiredPolicy string

const (
	// ExpiredSkip omits expired keys/fields from the plan and records them in
	// RestorePlan.SkippedExpired plus a warning. This is the default.
	ExpiredSkip ExpiredPolicy = "skip"
	// ExpiredKeep restores expired keys/fields anyway (with their original
	// absolute expiration preserved). Each one gets an explicit warning.
	ExpiredKeep ExpiredPolicy = "keep"
	// ExpiredError aborts planning with an *ExpiredKeyError as soon as an
	// expired key is encountered.
	ExpiredError ExpiredPolicy = "error"
)

// ExpiryMode decides how expiration is expressed in the generated commands.
type ExpiryMode string

const (
	// ExpiryAbsolute replays the original expiration timestamp with PEXPIREAT
	// / HPEXPIREAT. It is independent of the reference time and the default.
	ExpiryAbsolute ExpiryMode = "absolute"
	// ExpiryRelative converts the remaining lifetime at Options.ReferenceTime
	// into a PEXPIRE / HPEXPIRE (milliseconds) argument.
	ExpiryRelative ExpiryMode = "relative"
)

// ItemKind classifies a PlanItem.
type ItemKind string

const (
	KindKey       ItemKind = "key"
	KindFunctions ItemKind = "functions"
)

// ItemPhase groups commands belonging to one item.
type ItemPhase string

const (
	// PhaseCreate contains the commands which create the logical object
	// (e.g. SET / RPUSH / XADD entries / XGROUP CREATE). Commands inside one
	// phase share the item's atomicity boundary.
	PhaseCreate ItemPhase = "create"
	// PhaseFollowUp contains commands which only make sense after the object
	// exists (PEXPIREAT, XCLAIM, XSETID, ...).
	PhaseFollowUp ItemPhase = "followup"
)

// CommandRole distinguishes object creation from follow-up commands.
type CommandRole string

const (
	RoleCreate    CommandRole = "create"
	RoleFollowUp  CommandRole = "followup"
	RoleSelectDB  CommandRole = "select-db"
	RoleFunctions CommandRole = "functions"
)

// ItemStatus marks executability of an item.
type ItemStatus string

const (
	// StatusReady: every logical part of the item is restorable.
	StatusReady ItemStatus = "ready"
	// StatusLossy: the item can be restored but part of its semantics cannot
	// be reproduced (see item.Warnings / item.PartialBlockers).
	StatusLossy ItemStatus = "lossy"
	// StatusBlocked: the item cannot be restored on the target profile. The
	// item carries no executable commands and an explicit Blocker.
	StatusBlocked ItemStatus = "blocked"
	// StatusSkippedExpired: the key was already expired at the reference time
	// and ExpiredSkip removed it.
	StatusSkippedExpired ItemStatus = "skipped-expired"
)

// Options controls planning. The zero value is valid: it uses ExpiredSkip,
// ExpiryAbsolute and Redis74.
type Options struct {
	// ReferenceTime is the wall-clock instant at which the plan is produced.
	// It is required for ExpiryRelative and for expired-key detection.
	// When zero in ExpiryAbsolute mode it defaults to time.Now().
	ReferenceTime time.Time
	// ExpiredKeys controls expired key/field handling.
	ExpiredKeys ExpiredPolicy
	// ExpiryMode selects absolute vs. relative replay of expirations.
	ExpiryMode ExpiryMode
	// Target selects the Redis/Valkey capability profile. Nil means Redis74.
	Target *CapabilityProfile
	// MaxArgsPerCommand, when > 0, splits large create commands into chunks
	// (still within one atomicity group). 0 means a single command regardless
	// of argument count.
	MaxArgsPerCommand int
}

// Arg is a binary-safe command argument. It marshals to a base64 string so
// serialized plans never contain raw/non-UTF8 bytes.
type Arg []byte

// MarshalJSON encodes the argument as standard base64 text.
func (a Arg) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(a))
}

// Text returns the argument as a string; binary keys may look unreadable.
func (a Arg) Text() string { return string(a) }

// Command is a single Redis command. Dependencies reference Step.ID values.
type Command struct {
	Name string      `json:"name"`
	Args []Arg       `json:"args"`
	Role CommandRole `json:"role"`
	// DependsOn lists step IDs that must complete before this command runs.
	DependsOn []string `json:"dependsOn,omitempty"`
	// Notes carries deterministic, human-oriented remarks for that command.
	Notes []string `json:"notes,omitempty"`
}

// TextArgs returns the command name + args decoded to strings.
func (c *Command) TextArgs() []string {
	out := make([]string, 0, len(c.Args)+1)
	out = append(out, c.Name)
	for _, a := range c.Args {
		out = append(out, a.Text())
	}
	return out
}

// String renders a shell-like representation for logs. It is deterministic.
func (c *Command) String() string {
	return fmt.Sprintf("%v", c.TextArgs())
}

// Step is one numbered entry of the flat, ordered execution list.
type Step struct {
	ID     string    `json:"id"`
	DB     int       `json:"db"`
	ItemID string    `json:"itemId"`
	Phase  ItemPhase `json:"phase,omitempty"`
	Cmd    *Command  `json:"cmd"`
}

// Atomicity documents the transaction boundary of one logical item.
type Atomicity struct {
	// Atomic is true only when the whole item is a single command.
	Atomic bool `json:"atomic"`
	// NumCommands is the total number of commands implementing the item.
	NumCommands int `json:"numCommands"`
	// Boundary explains why atomicity stops where it stops.
	Boundary string `json:"boundary"`
}

// ExpirySpec describes how the key-level expiration is replayed.
type ExpirySpec struct {
	Mode ExpiryMode `json:"mode"`
	// AbsoluteAtMs is the original expiration as a Unix millisecond timestamp.
	AbsoluteAtMs int64 `json:"absoluteAtMs,omitempty"`
	// RelativeTTLMs is the remaining lifetime in ms at ReferenceTime
	// (Mode == ExpiryRelative). Zero/negative means already expired.
	RelativeTTLMs int64  `json:"relativeTtlMs,omitempty"`
	Precision     string `json:"precision"` // "millisecond"
	Expired       bool   `json:"expired"`
}

// Source locates the element inside the originating RDB/input. None of its
// fields depend on Go map iteration or on the order of the slice passed to
// the planner, so identical RDB content yields identical plans.
type Source struct {
	Kind     string `json:"kind"`               // "key" | "functions" | "aux"
	DB       int    `json:"db,omitempty"`       // key db index
	Key      string `json:"key,omitempty"`      // key (text; binary-safe)
	Encoding string `json:"encoding,omitempty"` // RDB encoding
	Type     string `json:"type,omitempty"`     // redis object type / module type
}

// Warning records a non-fatal, lossy aspect of an item or the whole plan.
type Warning struct {
	Code     string   `json:"code"`
	ItemID   string   `json:"itemId,omitempty"`
	Message  string   `json:"message"`
	Evidence []string `json:"evidence,omitempty"`
}

// Blocker records why something cannot be losslessly restored. Fatal blockers
// make an item StatusBlocked; non-fatal blockers downgrade it to lossy.
type Blocker struct {
	Code     string   `json:"code"`
	ItemID   string   `json:"itemId,omitempty"`
	Reason   string   `json:"reason"`
	Evidence []string `json:"evidence,omitempty"`
	// RequiredCapability, when set, names the capability the target lacks.
	RequiredCapability string `json:"requiredCapability,omitempty"`
	// Fatal means no executable command can implement the item.
	Fatal bool `json:"fatal"`
}

// PlanItem is one logical restorable element (a key or a function library).
type PlanItem struct {
	ID              string      `json:"id"`
	Kind            ItemKind    `json:"kind"`
	DB              int         `json:"db"`
	Key             string      `json:"key,omitempty"`
	Type            string      `json:"type,omitempty"`
	Encoding        string      `json:"encoding,omitempty"`
	Status          ItemStatus  `json:"status"`
	Source          Source      `json:"source"`
	Create          []*Command  `json:"createCommands,omitempty"`
	FollowUps       []*Command  `json:"followUpCommands,omitempty"`
	Expiry          *ExpirySpec `json:"expiry,omitempty"`
	Atomicity       Atomicity   `json:"atomicity"`
	Warnings        []Warning   `json:"warnings,omitempty"`
	PartialBlockers []Blocker   `json:"partialBlockers,omitempty"`
	Blocker         *Blocker    `json:"blocker,omitempty"`
}

// SkippedKey records an element removed from the execution plan.
type SkippedKey struct {
	ItemID string `json:"itemId"`
	DB     int    `json:"db"`
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

// RestorePlan is the complete, deterministic and serializable plan.
type RestorePlan struct {
	ReferenceTimeMs int64         `json:"referenceTimeMs"`
	Target          string        `json:"target"`
	ExpiryMode      ExpiryMode    `json:"expiryMode"`
	ExpiredPolicy   ExpiredPolicy `json:"expiredPolicy"`
	// DBS lists every touched db index in ascending order.
	DBS []int `json:"dbs"`
	// Items is ordered by (DB asc, key bytes asc, kind).
	Items []*PlanItem `json:"items"`
	// Steps is the flat, numbered, ordered execution script.
	Steps []*Step `json:"steps"`
	// SkippedExpired lists keys/fields removed under ExpiredSkip.
	SkippedExpired []SkippedKey `json:"skippedExpired,omitempty"`
	Warnings       []Warning    `json:"warnings,omitempty"`
	// Blockers aggregates fatal item blockers plus plan-level blockers.
	Blockers []Blocker `json:"blockers,omitempty"`
}

// ExpiredKeyError is returned by Generate with ExpiredPolicy == ExpiredError
// when an expired element is encountered.
type ExpiredKeyError struct {
	DB          int
	Key         string
	Field       string
	ExpiredAtMs int64
	RefMs       int64
}

func (e *ExpiredKeyError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("restoreplan: hash field %q of key %q in db %d expired at %d (reference %d)",
			e.Field, e.Key, e.DB, e.ExpiredAtMs, e.RefMs)
	}
	return fmt.Sprintf("restoreplan: key %q in db %d expired at %d (reference %d)",
		e.Key, e.DB, e.ExpiredAtMs, e.RefMs)
}
