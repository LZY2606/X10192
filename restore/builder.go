package restore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Builder collects decoded RDB objects and produces a RestorePlan.
type Builder struct {
	refTime    time.Time
	hasRefTime bool
	policy     ExpiredPolicy
	profile    *Profile
	objects    []model.RedisObject
}

// Option configures a Builder.
type Option func(*Builder)

// WithReferenceTime sets the restore reference time. Absolute expirations
// and relative TTLs in the plan are expressed against this time, and the
// expired-key policy is evaluated at this time. It is required for
// determinism: Build fails if it was never set.
func WithReferenceTime(t time.Time) Option {
	return func(b *Builder) {
		b.refTime = t
		b.hasRefTime = true
	}
}

// WithExpiredPolicy sets the policy for keys expired at the reference time.
// The default is ExpiredPolicySkip.
func WithExpiredPolicy(p ExpiredPolicy) Option {
	return func(b *Builder) { b.policy = p }
}

// WithProfile sets the target capability profile. The default is
// ProfileRedis74.
func WithProfile(p *Profile) Option {
	return func(b *Builder) {
		if p != nil {
			b.profile = p
		}
	}
}

// NewBuilder creates a Builder.
func NewBuilder(opts ...Option) *Builder {
	b := &Builder{
		policy:  ExpiredPolicySkip,
		profile: ProfileRedis74,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Add appends one decoded object to the plan input. Nil objects are ignored.
// The order of Add calls does not affect the resulting plan.
func (b *Builder) Add(obj model.RedisObject) {
	if obj == nil {
		return
	}
	b.objects = append(b.objects, obj)
}

// Plan builds a RestorePlan from already decoded objects.
func Plan(objects []model.RedisObject, opts ...Option) (*RestorePlan, error) {
	b := NewBuilder(opts...)
	for _, obj := range objects {
		b.Add(obj)
	}
	return b.Build()
}

// itemDraft is a plan item before stable IDs are assigned.
type itemDraft struct {
	role       Role
	commands   []Command
	expiration *ExpirationPlan
	fieldExps  []FieldExpiration
	notes      []string
}

// groupDraft is a logical group before stable IDs are assigned.
type groupDraft struct {
	db       int
	key      string
	typ      string
	encoding string
	items    []itemDraft
	hash     string // content hash, canonical-order tiebreak
	warnings []*Warning
	blockers []*Blocker
}

// Build produces the deterministic RestorePlan.
func (b *Builder) Build() (*RestorePlan, error) {
	if !b.hasRefTime {
		return nil, errors.New("restore: reference time is required, use WithReferenceTime")
	}
	switch b.policy {
	case ExpiredPolicySkip, ExpiredPolicyKeep, ExpiredPolicyError:
	default:
		return nil, fmt.Errorf("restore: unknown expired policy %q", b.policy)
	}

	plan := &RestorePlan{
		Version:       FormatVersion,
		ReferenceTime: b.refTime.UTC(),
		ExpiredPolicy: b.policy,
		Profile:       b.profile.Name,
	}
	var drafts []*groupDraft
	expiredSkipped := 0
	metadataSkipped := 0
	for _, obj := range b.objects {
		switch obj.(type) {
		case *model.AuxObject, *model.DBSizeObject:
			// RDB metadata is not restorable data; it is counted, not planned.
			metadataSkipped++
			continue
		}
		draft, skipped := b.planObject(obj, plan)
		if skipped {
			expiredSkipped++
			continue
		}
		if draft != nil {
			drafts = append(drafts, draft)
		}
	}
	// Canonical order: independent of input event order and map iteration.
	sort.SliceStable(drafts, func(i, j int) bool {
		if drafts[i].db != drafts[j].db {
			return drafts[i].db < drafts[j].db
		}
		if drafts[i].key != drafts[j].key {
			return drafts[i].key < drafts[j].key
		}
		if typeRank(drafts[i].typ) != typeRank(drafts[j].typ) {
			return typeRank(drafts[i].typ) < typeRank(drafts[j].typ)
		}
		return drafts[i].hash < drafts[j].hash
	})

	itemSeq := 0
	nextItemID := func() string {
		id := fmt.Sprintf("item-%06d", itemSeq)
		itemSeq++
		return id
	}
	currentDB := -1
	var selectID string
	for gi, draft := range drafts {
		group := &Group{
			ID:       fmt.Sprintf("grp-%06d", gi),
			DB:       draft.db,
			Key:      draft.key,
			Type:     draft.typ,
			Encoding: draft.encoding,
		}
		if len(draft.items) == 0 {
			group.Atomicity = "none: object cannot be restored, see blockers"
		} else if len(draft.items) == 1 && len(draft.items[0].commands) == 1 {
			group.Atomic = true
			group.Atomicity = "atomic: single command"
		} else {
			group.Atomicity = fmt.Sprintf("non-atomic: %d commands executed as independent commands; concurrent clients may observe partial state, wrap in MULTI/EXEC or a function if atomicity is required", countCommands(draft.items))
		}
		if len(draft.items) > 0 && draft.db != currentDB {
			currentDB = draft.db
			selectID = nextItemID()
			plan.Items = append(plan.Items, &PlanItem{
				ID:   selectID,
				Role: RoleSelectDB,
				DB:   draft.db,
				Source: Source{
					Ordinal: len(plan.Items) - 1,
					Locator: fmt.Sprintf("db%d", draft.db),
				},
				Commands: []Command{Cmd("SELECT", []byte(fmt.Sprintf("%d", draft.db)))},
			})
		}
		var prevID string
		for _, it := range draft.items {
			item := &PlanItem{
				ID:               nextItemID(),
				GroupID:          group.ID,
				Role:             it.role,
				DB:               draft.db,
				Key:              draft.key,
				Type:             draft.typ,
				Commands:         it.commands,
				Expiration:       it.expiration,
				FieldExpirations: it.fieldExps,
				Notes:            it.notes,
			}
			item.Source = Source{
				Ordinal: len(plan.Items),
				Locator: fmt.Sprintf("db%d:key=%q:type=%s", draft.db, draft.key, draft.typ),
			}
			if prevID != "" {
				item.DependsOn = []string{prevID}
			} else if selectID != "" {
				item.DependsOn = []string{selectID}
			}
			prevID = item.ID
			group.ItemIDs = append(group.ItemIDs, item.ID)
			plan.Items = append(plan.Items, item)
		}
		for _, w := range draft.warnings {
			w.GroupID = group.ID
			w.Key = draft.key
			plan.Warnings = append(plan.Warnings, w)
		}
		for _, bl := range draft.blockers {
			bl.GroupID = group.ID
			bl.Key = draft.key
			plan.Blockers = append(plan.Blockers, bl)
		}
		plan.Groups = append(plan.Groups, group)
	}
	// Warnings and blockers raised outside any group (e.g. expired-key
	// policy decisions) are sorted for determinism; grouped ones already
	// follow canonical group order.
	sort.SliceStable(plan.Warnings, func(i, j int) bool {
		if plan.Warnings[i].GroupID != plan.Warnings[j].GroupID {
			return plan.Warnings[i].GroupID < plan.Warnings[j].GroupID
		}
		if plan.Warnings[i].Code != plan.Warnings[j].Code {
			return plan.Warnings[i].Code < plan.Warnings[j].Code
		}
		return plan.Warnings[i].Message < plan.Warnings[j].Message
	})
	sort.SliceStable(plan.Blockers, func(i, j int) bool {
		if plan.Blockers[i].GroupID != plan.Blockers[j].GroupID {
			return plan.Blockers[i].GroupID < plan.Blockers[j].GroupID
		}
		if plan.Blockers[i].Code != plan.Blockers[j].Code {
			return plan.Blockers[i].Code < plan.Blockers[j].Code
		}
		return plan.Blockers[i].Message < plan.Blockers[j].Message
	})

	dbSet := make(map[int]struct{})
	for _, g := range plan.Groups {
		dbSet[g.DB] = struct{}{}
	}
	for db := range dbSet {
		plan.DBs = append(plan.DBs, db)
	}
	sort.Ints(plan.DBs)

	plan.Stats = Stats{
		Objects:         len(b.objects),
		Groups:          len(plan.Groups),
		Items:           len(plan.Items),
		ExpiredSkipped:  expiredSkipped,
		MetadataSkipped: metadataSkipped,
		Warnings:        len(plan.Warnings),
		Blockers:        len(plan.Blockers),
	}
	for _, item := range plan.Items {
		plan.Stats.Commands += len(item.Commands)
	}
	return plan, nil
}

func countCommands(items []itemDraft) int {
	n := 0
	for _, it := range items {
		n += len(it.commands)
	}
	return n
}

func typeRank(typ string) int {
	switch typ {
	case model.StringType:
		return 0
	case model.ListType:
		return 1
	case model.SetType:
		return 2
	case model.HashType:
		return 3
	case model.ZSetType:
		return 4
	case model.StreamType:
		return 5
	case model.FunctionsType:
		return 6
	}
	return 7
}

// contentHash is a deterministic fingerprint used as a canonical-order
// tiebreak. encoding/json sorts map keys, so it is map-iteration safe.
func contentHash(obj model.RedisObject) string {
	raw, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// expirationFor builds the expiration plan and PEXPIREAT command for a key.
func (b *Builder) expirationFor(obj model.RedisObject) (*ExpirationPlan, Command) {
	exp := obj.GetExpiration()
	absMs := exp.UnixNano() / int64(time.Millisecond)
	ttlMs := absMs - b.refTime.UnixNano()/int64(time.Millisecond)
	ep := &ExpirationPlan{
		AbsoluteUnixMs:     absMs,
		TTLMsAtReference:   ttlMs,
		ExpiredAtReference: ttlMs <= 0,
		Semantics:          "absolute expiration replayed as PEXPIREAT and valid at any execution time; TTLMsAtReference is the remaining TTL at the plan reference time for PEXPIRE-style replay",
	}
	cmd := Cmd("PEXPIREAT", []byte(obj.GetKey()), []byte(fmt.Sprintf("%d", absMs)))
	return ep, cmd
}

// expiredAt reports whether the object is expired at the reference time.
func (b *Builder) expiredAt(obj model.RedisObject) bool {
	exp := obj.GetExpiration()
	return exp != nil && !exp.After(b.refTime)
}

// applyExpiredPolicy handles keys expired at the reference time. It reports
// whether the object should be restored at all, and whether its expiration
// should be restored.
func (b *Builder) applyExpiredPolicy(obj model.RedisObject, draft *groupDraft) (restore bool, withTTL bool) {
	if !b.expiredAt(obj) {
		return true, true
	}
	key := obj.GetKey()
	expiredAt := obj.GetExpiration().UTC().Format(time.RFC3339Nano)
	refAt := b.refTime.UTC().Format(time.RFC3339Nano)
	switch b.policy {
	case ExpiredPolicySkip:
		draft.warnings = append(draft.warnings, &Warning{
			Code:    "expired-key-skipped",
			Message: fmt.Sprintf("key expired at %s (reference time %s), skipped by policy", expiredAt, refAt),
		})
		return false, false
	case ExpiredPolicyError:
		draft.blockers = append(draft.blockers, &Blocker{
			Code:     "expired-key",
			Message:  fmt.Sprintf("key expired at %s (reference time %s)", expiredAt, refAt),
			Evidence: "expired policy is \"error\"",
		})
		return false, false
	case ExpiredPolicyKeep:
		draft.warnings = append(draft.warnings, &Warning{
			Code:    "expired-key-kept-without-ttl",
			Message: fmt.Sprintf("key expired at %s (reference time %s), restored without expiration by policy", expiredAt, refAt),
		})
		return true, false
	}
	return true, true
}
