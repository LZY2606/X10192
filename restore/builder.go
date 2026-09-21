package restore

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Event wraps a decoded object with optional provenance supplied by the
// caller. Only Object is required; Offset/Sequence/Precision are recorded in
// Source but never affect ordering or stable ids.
type Event struct {
	// Object is the decoded RDB object.
	Object model.RedisObject
	// Offset is an optional byte offset into the source RDB.
	Offset int64
	// Sequence is an optional source ordinal (e.g. decoder event number).
	Sequence int64
	// Precision records the original key expiration opcode precision:
	// "second" or "millisecond". Empty defaults to "millisecond".
	Precision string
}

// Options configures plan generation.
type Options struct {
	// ReferenceTime is the instant used to convert absolute expiration times
	// into remaining TTLs. It must be set explicitly; the zero value is
	// rejected so plans can never depend on the machine clock.
	ReferenceTime time.Time
	// ExpiredPolicy selects skip/keep/error for already-expired keys.
	ExpiredPolicy ExpiredPolicy
	// Target selects the Redis capability profile. Nil means DefaultProfile.
	Target *CapabilityProfile
}

const (
	kindKey       = "key"
	kindFunctions = "functions"

	nodeKindCreate          = "create"
	nodeKindExpire          = "expire"
	nodeKindGroup           = "group"
	nodeKindConsumer        = "consumer"
	nodeKindPEL             = "pel"
	nodeKindFunctions       = "functions"
	nodeKindEmptyWorkaround = "empty-stream-workaround"
)

// BuildPlan turns decoded objects into a deterministic RestorePlan.
//
// It never connects to Redis and never executes commands. The input slice may
// be in any order: items are sorted by content. Likewise, values carried in
// Go maps (hash fields, set members in plan output) are sorted before use.
func BuildPlan(objs []model.RedisObject, opts Options) (*RestorePlan, error) {
	events := make([]Event, 0, len(objs))
	for _, obj := range objs {
		events = append(events, Event{Object: obj})
	}
	return BuildPlanFromEvents(events, opts)
}

// BuildPlanFromEvents is BuildPlan with optional source positions and the
// original key expiration precision.
func BuildPlanFromEvents(events []Event, opts Options) (*RestorePlan, error) {
	if opts.ReferenceTime.IsZero() {
		return nil, errors.New("restore: ReferenceTime is required and must be non-zero")
	}
	if opts.ExpiredPolicy == "" {
		opts.ExpiredPolicy = ExpiredPolicySkip
	}
	switch opts.ExpiredPolicy {
	case ExpiredPolicySkip, ExpiredPolicyKeep, ExpiredPolicyError:
	default:
		return nil, fmt.Errorf("restore: unknown ExpiredPolicy %q", opts.ExpiredPolicy)
	}
	if opts.Target == nil {
		opts.Target = DefaultProfile
	}

	b := &builder{
		opts:    opts,
		warning: make(map[string]*Warning),
	}

	var expiredBlocked bool
	for _, ev := range events {
		obj := ev.Object
		if obj == nil {
			continue
		}
		// Metadata opcodes (aux, dbsize) are not restore payload.
		switch obj.GetType() {
		case model.AuxType, model.DBSizeType:
			continue
		}
		item := b.planEvent(ev)
		if item != nil && item.Status == ItemStatusBlocked && hasCode(item, WarnKeyExpiredBlocked) {
			expiredBlocked = true
		}
	}

	b.finalize()

	plan := &RestorePlan{
		Version:       RestorePlanVersion,
		ReferenceTime: opts.ReferenceTime.UTC(),
		ExpiredPolicy: opts.ExpiredPolicy,
		Target:        opts.Target,
		Items:         b.items,
		Databases:     b.databases(),
		Warnings:      b.warningList(),
		Stats:         b.stats,
		Executable:    b.stats.BlockerCount == 0,
	}
	if plan.Items == nil {
		plan.Items = []*PlanItem{}
	}
	if plan.Databases == nil {
		plan.Databases = []*DatabasePlan{}
	}
	if plan.Warnings == nil {
		plan.Warnings = []*Warning{}
	}

	if expiredBlocked {
		return plan, errors.New("restore: one or more keys were already expired at reference time under policy=error")
	}
	return plan, nil
}

type builder struct {
	opts    Options
	items   []*PlanItem
	stats   PlanStats
	warning map[string]*Warning
}

func hasCode(item *PlanItem, code string) bool {
	for _, id := range item.WarningIDs {
		if warningCodeOf(id) == code {
			return true
		}
	}
	return false
}

// warningCodeOf extracts the code embedded in a stable warning id
// (shape: "w/<code>/<hash>").
func warningCodeOf(id string) string {
	rest := id
	if len(rest) >= 2 && rest[0:2] == "w/" {
		rest = rest[2:]
	}
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i]
		}
	}
	return rest
}

// addWarning aggregates a finding. Evidence is a sorted set of item/node ids.
func (b *builder) addWarning(code string, severity WarningSeverity, message string, evidence ...string) string {
	sort.Strings(evidence)
	id := warningID(code, evidence)
	w, ok := b.warning[id]
	if !ok {
		w = &Warning{
			ID:       id,
			Code:     code,
			Severity: severity,
			Message:  message,
			Evidence: evidence,
		}
		b.warning[id] = w
		switch severity {
		case SeverityError:
			b.stats.BlockerCount++
		default:
			b.stats.WarningCount++
		}
		return id
	}
	// Merge evidence defensively (IDs are content-addressed so this is rare).
	w.Evidence = mergeSorted(w.Evidence, evidence)
	return id
}

func mergeSorted(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		set[s] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func (b *builder) planEvent(ev Event) *PlanItem {
	obj := ev.Object
	if obj.GetType() == model.FunctionsType {
		return b.planFunctions(ev)
	}

	item := &PlanItem{
		ID:       keyItemID(obj.GetDBIndex(), obj.GetKey()),
		Kind:     kindKey,
		DB:       obj.GetDBIndex(),
		Key:      obj.GetKey(),
		Type:     obj.GetType(),
		Encoding: obj.GetEncoding(),
		Source: Source{
			DB:       obj.GetDBIndex(),
			Key:      obj.GetKey(),
			Offset:   ev.Offset,
			Sequence: ev.Sequence,
		},
		Status: ItemStatusActive,
	}

	b.planKeyContent(item, obj)
	b.planKeyExpiration(item, obj, ev.Precision)

	b.items = append(b.items, item)
	b.stats.ItemCount++
	switch item.Status {
	case ItemStatusBlocked:
		b.stats.BlockedItems++
	case ItemStatusSkipped:
		b.stats.SkippedItems++
	default:
		b.stats.ActiveItems++
	}
	return item
}

// finalize sorts items into stable execution order and numbers nodes.
func (b *builder) finalize() {
	functionsFirstThenDBKey := func(i, j int) bool {
		a, c := b.items[i], b.items[j]
		if a.Kind != c.Kind {
			return a.Kind == kindFunctions
		}
		if a.DB != c.DB {
			return a.DB < c.DB
		}
		return a.Key < c.Key
	}
	sort.Slice(b.items, func(i, j int) bool { return functionsFirstThenDBKey(i, j) })

	var functionsID string
	for _, it := range b.items {
		if it.Kind == kindFunctions {
			functionsID = it.ID
		}
	}
	for _, it := range b.items {
		if it.DependsOn == nil {
			it.DependsOn = []string{}
		}
		if it.WarningIDs == nil {
			it.WarningIDs = []string{}
		}
		for _, n := range it.Nodes {
			if n.DependsOn == nil {
				n.DependsOn = []string{}
			}
			if n.Groups == nil {
				n.Groups = []*CommandGroup{}
			}
			if n.Notes == nil {
				n.Notes = []string{}
			}
			if n.WarningIDs == nil {
				n.WarningIDs = []string{}
			}
		}
		if it.Kind == kindKey && functionsID != "" && len(it.Nodes) > 0 {
			it.DependsOn = appendUnique(it.DependsOn, functionsID)
		}
		// Stable node order: already appended in dependency order by planners,
		// but sort defensively by id to remove any map-driven variance.
		sort.SliceStable(it.Nodes, func(i, j int) bool {
			return nodeOrder(it, it.Nodes[i], it.Nodes[j])
		})
		sort.Strings(it.DependsOn)
		it.WarningIDs = dedupSorted(it.WarningIDs)
		for _, n := range it.Nodes {
			sort.Strings(n.DependsOn)
			n.WarningIDs = dedupSorted(n.WarningIDs)
			b.stats.NodeCount++
			for _, g := range n.Groups {
				b.stats.CommandCount += len(g.Commands)
			}
		}
	}
}

// nodeOrder keeps create/expire first and otherwise sorts by node id.
func nodeOrder(item *PlanItem, a, c *PlanNode) bool {
	rank := func(n *PlanNode) int {
		switch n.Kind {
		case nodeKindCreate, nodeKindEmptyWorkaround:
			return 0
		case nodeKindExpire:
			return 1
		case nodeKindGroup:
			return 2
		case nodeKindConsumer:
			return 3
		case nodeKindPEL:
			return 4
		default:
			return 5
		}
	}
	ra, rc := rank(a), rank(c)
	if ra != rc {
		return ra < rc
	}
	return a.ID < c.ID
}

func (b *builder) databases() []*DatabasePlan {
	byDB := make(map[int][]string)
	for _, it := range b.items {
		if it.Kind != kindKey {
			continue
		}
		byDB[it.DB] = append(byDB[it.DB], it.ID)
	}
	dbs := make([]int, 0, len(byDB))
	for db := range byDB {
		dbs = append(dbs, db)
	}
	sort.Ints(dbs)
	out := make([]*DatabasePlan, 0, len(dbs))
	for _, db := range dbs {
		ids := byDB[db]
		sort.Strings(ids)
		out = append(out, &DatabasePlan{
			DB:            db,
			SwitchCommand: &Command{Args: []string{"SELECT", strconv.Itoa(db)}},
			ItemIDs:       ids,
		})
	}
	b.stats.TouchedDBCount = len(out)
	return out
}

func (b *builder) warningList() []*Warning {
	out := make([]*Warning, 0, len(b.warning))
	for _, w := range b.warning {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func dedupSorted(s []string) []string {
	if len(s) < 2 {
		return s
	}
	sort.Strings(s)
	out := s[:0]
	for _, v := range s {
		if len(out) > 0 && out[len(out)-1] == v {
			continue
		}
		out = append(out, v)
	}
	return out
}

// blocked marks an item and records an error-severity finding.
func (b *builder) block(item *PlanItem, code, message string) {
	item.Status = ItemStatusBlocked
	id := b.addWarning(code, SeverityError, message, item.ID)
	item.WarningIDs = appendUnique(item.WarningIDs, id)
}

// warn records a warning-severity finding attached to an item/node.
func (b *builder) warnItem(item *PlanItem, node *PlanNode, code, message string, evidence ...string) {
	ev := evidence
	if len(ev) == 0 {
		// Default evidence is the item so repeated same-code findings on
		// different nodes (e.g. one per consumer) aggregate deterministically
		// into a single warning.
		ev = []string{item.ID}
	}
	id := b.addWarning(code, SeverityWarning, message, ev...)
	item.WarningIDs = appendUnique(item.WarningIDs, id)
	if node != nil {
		node.WarningIDs = appendUnique(node.WarningIDs, id)
	}
}

// sortedByteStrings returns the strings sorted by raw byte order.
func sortedByteStrings(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
