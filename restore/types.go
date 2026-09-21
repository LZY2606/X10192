package restore

import (
	"encoding/base64"
	"time"
	"unicode/utf8"
)

// CapabilityProfile describes commands available on the target Redis server.
type CapabilityProfile struct {
	Name                     string `json:"name"`
	Version                  string `json:"version"`
	Functions                bool   `json:"functions"`
	HashFieldExpiration      bool   `json:"hashFieldExpiration"`
	Streams                  bool   `json:"streams"`
	StreamConsumerCreate     bool   `json:"streamConsumerCreate"`
	StreamEntriesRead        bool   `json:"streamEntriesRead"`
	StreamConsumerActiveTime bool   `json:"streamConsumerActiveTime"`
}

// Redis5Profile targets Redis 5.0, where streams exist but functions and HFE do not.
func Redis5Profile() CapabilityProfile {
	return CapabilityProfile{Name: "redis", Version: "5.0", Streams: true}
}

// Redis6Profile targets Redis 6.2.
func Redis6Profile() CapabilityProfile {
	return CapabilityProfile{Name: "redis", Version: "6.2", Streams: true, StreamConsumerCreate: true}
}

// Redis7Profile targets Redis 7.0.
func Redis7Profile() CapabilityProfile {
	p := Redis6Profile()
	p.Version = "7.0"
	p.Functions = true
	p.StreamEntriesRead = true
	return p
}

// Redis72Profile targets Redis 7.2.
func Redis72Profile() CapabilityProfile {
	p := Redis7Profile()
	p.Version = "7.2"
	return p
}

// Redis74Profile targets Redis 7.4.
func Redis74Profile() CapabilityProfile {
	p := Redis72Profile()
	p.Version = "7.4"
	p.HashFieldExpiration = true
	p.StreamConsumerActiveTime = true
	return p
}

// ExpiredKeyPolicy decides what to do when a key is expired at the reference time.
type ExpiredKeyPolicy string

const (
	ExpiredKeySkip  ExpiredKeyPolicy = "skip"
	ExpiredKeyKeep  ExpiredKeyPolicy = "keep-without-ttl"
	ExpiredKeyError ExpiredKeyPolicy = "error"
)

// ExpirationMode selects the command form used for key TTLs.
type ExpirationMode string

const (
	ExpirationAbsolute ExpirationMode = "absolute"
	ExpirationRelative ExpirationMode = "relative"
)

// Options controls deterministic planning.
type Options struct {
	ReferenceTime  time.Time         `json:"referenceTime"`
	Profile        CapabilityProfile `json:"profile"`
	ExpiredKeys    ExpiredKeyPolicy  `json:"expiredKeys"`
	ExpirationMode ExpirationMode    `json:"expirationMode"`
	WarnMetadata   bool              `json:"warnMetadata"`
}

// Arg keeps textual Redis arguments directly readable and binary arguments lossless.
type Arg struct {
	Text         string `json:"text,omitempty"`
	BinaryBase64 string `json:"binaryBase64,omitempty"`
}

func newArg(value []byte) Arg {
	if utf8.Valid(value) {
		return Arg{Text: string(value)}
	}
	return Arg{BinaryBase64: base64.StdEncoding.EncodeToString(value)}
}

func newStringArg(value string) Arg {
	return newArg([]byte(value))
}

// Source identifies the decoded object independently of input slice or map order.
type Source struct {
	Kind       string `json:"kind"`
	DB         int    `json:"db,omitempty"`
	ByteOffset int64  `json:"byteOffset,omitempty"`
	Key        string `json:"key,omitempty"`
	ObjectType string `json:"objectType,omitempty"`
	Encoding   string `json:"encoding,omitempty"`
}

// Finding is a structured, serializable warning or hard blocker.
type Finding struct {
	Code     string   `json:"code"`
	Message  string   `json:"message"`
	Source   Source   `json:"source"`
	Evidence []string `json:"evidence,omitempty"`
}

type Warning = Finding
type Blocker = Finding

const (
	PhaseSelect   = "select"
	PhaseCreate   = "create"
	PhaseFollowUp = "follow-up"
)

const (
	RoleSelect              = "select-db"
	RoleCreate              = "create"
	RoleExpiration          = "expiration"
	RoleHashFieldExpiration = "hash-field-expiration"
	RoleStreamGroup         = "stream-group"
	RoleStreamConsumer      = "stream-consumer"
	RoleStreamPEL           = "stream-pel"
	RoleFunction            = "function"
)

// PlannedCommand is one executable Redis command and its replay dependencies.
type PlannedCommand struct {
	ID        string   `json:"id"`
	ItemID    string   `json:"itemId"`
	DB        int      `json:"db"`
	Phase     string   `json:"phase"`
	Role      string   `json:"role"`
	Args      []Arg    `json:"args"`
	DependsOn []string `json:"dependsOn,omitempty"`
}

// Atomicity records the non-transaction boundary around a multi-command logical object.
type Atomicity struct {
	GroupID          string   `json:"groupId"`
	Boundary         string   `json:"boundary"`
	CommandIDs       []string `json:"commandIds"`
	NonAtomicBecause string   `json:"nonAtomicBecause,omitempty"`
}

// ExpirationPlan records both absolute RDB time and TTL measured from the reference time.
type ExpirationPlan struct {
	SourceUnit         string    `json:"sourceUnit,omitempty"`
	ExpireAt           time.Time `json:"expireAt"`
	ReferenceTime      time.Time `json:"referenceTime"`
	RelativeTTLMillis  int64     `json:"relativeTtlMillis"`
	CommandID          string    `json:"commandId,omitempty"`
	ExpiredAtReference bool      `json:"expiredAtReference"`
	Action             string    `json:"action"`
}

// FieldExpirationPlan describes one hash field's HFE state.
type FieldExpirationPlan struct {
	Field              Arg       `json:"field"`
	ExpireAt           time.Time `json:"expireAt"`
	ReferenceTime      time.Time `json:"referenceTime"`
	RelativeTTLMillis  int64     `json:"relativeTtlMillis"`
	SourceUnit         string    `json:"sourceUnit"`
	CommandID          string    `json:"commandId,omitempty"`
	ExpiredAtReference bool      `json:"expiredAtReference"`
	Action             string    `json:"action"`
}

const (
	ItemStatusActive  = "active"
	ItemStatusSkipped = "skipped"
	ItemStatusBlocked = "blocked"
)

// PlanItem is one logical RDB object, even when restoration requires several commands.
type PlanItem struct {
	ID               string                `json:"id"`
	Status           string                `json:"status"`
	Source           Source                `json:"source"`
	TargetDB         int                   `json:"targetDb"`
	Key              string                `json:"key,omitempty"`
	ObjectType       string                `json:"objectType"`
	Encoding         string                `json:"encoding"`
	CreateCommands   []PlannedCommand      `json:"createCommands,omitempty"`
	FollowUpCommands []PlannedCommand      `json:"followUpCommands,omitempty"`
	Expiration       *ExpirationPlan       `json:"expiration,omitempty"`
	FieldExpirations []FieldExpirationPlan `json:"fieldExpirations,omitempty"`
	LogicalGroup     Atomicity             `json:"logicalGroup"`
	Warnings         []Warning             `json:"warnings,omitempty"`
	Blockers         []Blocker             `json:"blockers,omitempty"`
}

// DatabasePlan explicitly represents the required SELECT boundary.
type DatabasePlan struct {
	DB           int      `json:"db"`
	SelectNodeID string   `json:"selectNodeId"`
	ItemIDs      []string `json:"itemIds"`
}

// RestorePlan is the deterministic, serializable planning result.
type RestorePlan struct {
	ReferenceTime time.Time         `json:"referenceTime"`
	Profile       CapabilityProfile `json:"profile"`
	Databases     []DatabasePlan    `json:"databases"`
	Items         []PlanItem        `json:"items,omitempty"`
	GlobalItems   []PlanItem        `json:"globalItems,omitempty"`
	Nodes         []PlannedCommand  `json:"nodes"`
	Warnings      []Warning         `json:"warnings,omitempty"`
	Blockers      []Blocker         `json:"blockers,omitempty"`
}
